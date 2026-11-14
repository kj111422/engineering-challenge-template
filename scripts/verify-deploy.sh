#!/usr/bin/env bash
# Deploy verification: prove the documented deployment path actually works.
#
# This is not a unit test and not a simulation. It builds the release
# binary, installs it into a sandbox that mirrors the layout from
# deploy/deployment-notes.md (etc/engineering-challenge/settings.toml,
# var/lib/engineering-challenge, var/log/engineering-challenge), starts the
# collector exactly as the systemd unit does (`--config <settings.toml>`,
# default command), exercises the documented health endpoints, waits for the
# bounded run to finish, and then verifies every artifact through the
# production integrity chain: SBE reconstruction, per-symbol completeness,
# coverage certification, and raw-frame journal verification.
#
# The public Polymarket feed is used because it needs no credentials; every
# other part of the path (config file, directory layout, health endpoints,
# artifact verification) is the documented production path.
#
# The systemd unit is verified structurally (hardening directives, non-root
# user, restart policy, ReadWritePaths). On a Linux host with
# systemd-analyze available, the unit is additionally parsed by it.
#
# Usage: scripts/verify-deploy.sh
# Output: build/deploy-verification/<stamp>/  and a deploy-verification.json
# manifest. Fail-closed: any failed check exits non-zero.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SANDBOX="$ROOT/build/deploy-verification/$STAMP"
ETC="$SANDBOX/etc/engineering-challenge"
VAR_LIB="$SANDBOX/var/lib/engineering-challenge"
VAR_LOG="$SANDBOX/var/log/engineering-challenge"
SETTINGS="$ETC/settings.toml"
DURATION_SECS="${DEPLOY_VERIFY_DURATION_SECS:-45}"
UNIT="deploy/engineering-challenge.service"

fail() { echo "ERROR: $*" >&2; exit 1; }
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

command -v cargo >/dev/null 2>&1 || fail "cargo is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"
command -v curl >/dev/null 2>&1 || fail "curl is required"
[[ -f "$UNIT" ]] || fail "$UNIT is missing"
[[ -f config.example.toml ]] || fail "config.example.toml is missing"

COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
DIRTY="$(git status --porcelain | head -1)"
mkdir -p "$ETC" "$VAR_LIB" "$VAR_LOG" || fail "cannot create sandbox"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "deploy-verification sandbox: $SANDBOX (commit ${COMMIT:0:12}${DIRTY:+, dirty})"

echo "== building release binary"
cargo build --release --locked >"$SANDBOX/build.log" 2>&1 ||
  { tail -n 40 "$SANDBOX/build.log" >&2; fail "release build failed"; }
BIN="$ROOT/target/release/engineering-challenge"
[[ -x "$BIN" ]] || fail "release binary not produced"

# A free localhost port for the metrics endpoint, chosen before the settings
# file is written so the scrape targets are deterministic.
METRICS_PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"

echo "== writing settings.toml from config.example.toml"
python3 - "$ROOT/config.example.toml" "$SETTINGS" "$VAR_LIB" "$METRICS_PORT" "$DURATION_SECS" <<'PY'
import re, sys
source, target, var_lib, port, duration = sys.argv[1:]
text = open(source, encoding="utf-8").read()
text = re.sub(r'^exchange = ".*"', 'exchange = "polymarket"', text, count=1, flags=re.M)
# Drop the Kalshi credential section: the sandbox uses the public feed.
text = re.sub(r'^\[kalshi\].*?(?=^\[)', "", text, flags=re.S | re.M)
text = re.sub(r'^duration_secs = 0$', f'duration_secs = {duration}', text, count=1, flags=re.M)
text = re.sub(r'^max_markets = 0$', 'max_markets = 5', text, count=1, flags=re.M)
text = text.replace('path = "captures/kalshi.sbe"', f'path = "{var_lib}/capture.sbe"')
text = text.replace('metadata_path = "captures/kalshi-markets.json"', f'metadata_path = "{var_lib}/markets.json"')
text = text.replace('report_path = "captures/kalshi-report.json"', f'report_path = "{var_lib}/report.json"')
text = re.sub(r'^listen = "127\.0\.0\.1:9090"$', f'listen = "127.0.0.1:{port}"', text, count=1, flags=re.M)
# Enable the raw-frame journal at a sandbox path.
text = re.sub(r'^# directory = "captures/kalshi-journal"$', f'directory = "{var_lib}/journal"', text, count=1, flags=re.M)
open(target, "w", encoding="utf-8").write(text)
PY
[[ -s "$SETTINGS" ]] || fail "settings.toml was not generated"
grep -q '^exchange = "polymarket"$' "$SETTINGS" || fail "settings patch failed (exchange)"
grep -q "^directory = \"$VAR_LIB/journal\"$" "$SETTINGS" || fail "settings patch failed (journal)"
grep -q "^duration_secs = $DURATION_SECS$" "$SETTINGS" || fail "settings patch failed (duration)"

echo "== structural checks on the systemd unit"
unit_checks=(
  "^User=collector$"
  "^Group=collector$"
  "^NoNewPrivileges=yes$"
  "^ProtectSystem=strict$"
  "^ProtectHome=yes$"
  "^ReadWritePaths=/var/lib/engineering-challenge /var/log/engineering-challenge$"
  "^Restart=on-failure$"
  "^MemoryMax=4G$"
  "^LimitNOFILE=65536$"
  "^ExecStart=/usr/local/bin/engineering-challenge --config /etc/engineering-challenge/settings.toml$"
)
UNIT_STATUS=pass
for pattern in "${unit_checks[@]}"; do
  if ! grep -qE "$pattern" "$UNIT"; then
    echo "unit hardening check FAILED: $pattern" >&2
    UNIT_STATUS=FAIL
  fi
done
if [[ "$UNIT_STATUS" == pass ]]; then
  echo "unit hardening: all 10 directives present"
else
  echo "unit hardening: FAILED" >&2
fi
if command -v systemd-analyze >/dev/null 2>&1; then
  systemd-analyze verify "$UNIT" 2>"$SANDBOX/systemd-analyze.log" &&
    echo "systemd-analyze verify: pass" ||
    { echo "systemd-analyze verify FAILED; see $SANDBOX/systemd-analyze.log" >&2; UNIT_STATUS=FAIL; }
else
  echo "systemd-analyze not available on this host; structural checks only"
fi

echo "== starting collector exactly as systemd does (--config, default command)"
RUST_LOG=info "$BIN" --config "$SETTINGS" \
  >"$VAR_LOG/collector.log" 2>&1 &
COLLECTOR_PID=$!

BASE_URL="http://127.0.0.1:$METRICS_PORT"
LIVEZ_STATUS=FAIL
READYZ_STATUS=FAIL
METRICS_STATUS=FAIL
READY_BODY=""
deadline=$((SECONDS + 150))
while (( SECONDS < deadline )); do
  if ! kill -0 "$COLLECTOR_PID" 2>/dev/null; then
    break
  fi
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$BASE_URL/livez" 2>/dev/null || true)"
  [[ "$code" == "200" ]] && LIVEZ_STATUS=pass
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$BASE_URL/readyz" 2>/dev/null || true)"
  if [[ "$code" == "200" ]]; then
    READYZ_STATUS=pass
    READY_BODY="$(curl -s --max-time 2 "$BASE_URL/readyz" 2>/dev/null || true)"
  fi
  if curl -s --max-time 2 "$BASE_URL/metrics" 2>/dev/null | grep -q '^# HELP collector_deltas_emitted_total'; then
    METRICS_STATUS=pass
  fi
  if [[ "$LIVEZ_STATUS" == pass && "$READYZ_STATUS" == pass && "$METRICS_STATUS" == pass ]]; then
    break
  fi
  sleep 2
done
echo "health: livez=$LIVEZ_STATUS readyz=$READYZ_STATUS metrics=$METRICS_STATUS ($READY_BODY)"

echo "== waiting for the bounded run to finish (duration_secs=$DURATION_SECS)"
EXIT_STATUS=FAIL
deadline=$((SECONDS + 300))
while (( SECONDS < deadline )); do
  if ! kill -0 "$COLLECTOR_PID" 2>/dev/null; then
    wait "$COLLECTOR_PID"
    collector_exit=$?
    if (( collector_exit == 0 )); then EXIT_STATUS=pass; fi
    echo "collector exited with status $collector_exit"
    break
  fi
  sleep 2
done
if [[ "$EXIT_STATUS" == FAIL ]] && kill -0 "$COLLECTOR_PID" 2>/dev/null; then
  echo "collector did not exit within the deadline; sending SIGTERM" >&2
  kill -TERM "$COLLECTOR_PID" 2>/dev/null || true
  sleep 10
  kill -KILL "$COLLECTOR_PID" 2>/dev/null || true
  wait "$COLLECTOR_PID" 2>/dev/null || true
fi

echo "== verifying artifacts through the production integrity chain"
for artifact in capture.sbe markets.json report.json; do
  [[ -s "$VAR_LIB/$artifact" ]] || { echo "artifact missing/empty: $VAR_LIB/$artifact" >&2; tail -n 40 "$VAR_LOG/collector.log" >&2; fail "deployed run did not produce $artifact"; }
done

"$BIN" verify "$VAR_LIB/capture.sbe" >"$SANDBOX/sbe-verify.json" 2>"$SANDBOX/sbe-verify.log" ||
  { echo "SBE verification FAILED; see $SANDBOX/sbe-verify.json" >&2; SBE_STATUS=FAIL; }
SBE_STATUS=${SBE_STATUS:-pass}
"$BIN" completeness "$VAR_LIB/capture.sbe" --out "$SANDBOX/completeness.json" >"$SANDBOX/completeness.stdout" 2>&1 ||
  { echo "completeness FAILED; see $SANDBOX/completeness.json" >&2; COMPLETE_STATUS=FAIL; }
COMPLETE_STATUS=${COMPLETE_STATUS:-pass}
"$BIN" coverage "$VAR_LIB/capture.sbe" "$VAR_LIB/markets.json" "$VAR_LIB/report.json" \
  --out "$SANDBOX/coverage.json" >"$SANDBOX/coverage.stdout" 2>&1 ||
  { echo "coverage FAILED; see $SANDBOX/coverage.json" >&2; COVERAGE_STATUS=FAIL; }
COVERAGE_STATUS=${COVERAGE_STATUS:-pass}

JOURNAL_RUN="$(python3 - "$VAR_LIB/report.json" <<'PY'
import json, sys
try:
    report = json.load(open(sys.argv[1], encoding="utf-8"))
    print((report.get("journal") or {}).get("run_directory", ""))
except Exception:
    print("")
PY
)"
if [[ -n "$JOURNAL_RUN" ]]; then
  [[ "$JOURNAL_RUN" == /* ]] || JOURNAL_RUN="$ROOT/$JOURNAL_RUN"
  "$BIN" journal verify "$JOURNAL_RUN" >"$SANDBOX/journal-verify.json" 2>"$SANDBOX/journal-verify.log" ||
    { echo "journal verification FAILED; see $SANDBOX/journal-verify.json" >&2; JOURNAL_STATUS=FAIL; }
  JOURNAL_STATUS=${JOURNAL_STATUS:-pass}
else
  echo "report does not name a journal run" >&2
  JOURNAL_STATUS=FAIL
fi

python3 - "$SANDBOX" "$VAR_LIB" <<'PY' || VERDICT=FAIL
import json, sys
sandbox, var_lib = sys.argv[1:]
load = lambda name: json.load(open(f"{sandbox}/{name}", encoding="utf-8"))
report = json.load(open(f"{var_lib}/report.json", encoding="utf-8"))
sbe, complete, coverage, journal = load("sbe-verify.json"), load("completeness.json"), load("coverage.json"), load("journal-verify.json")
checks = {
    "sbe_reconstruction_valid": sbe.get("valid") is True,
    "completeness_valid": complete.get("valid") is True,
    "coverage_valid": coverage.get("valid") is True,
    "coverage_anchored_equals_expected": int(coverage.get("expected", 0)) > 0
        and int(coverage.get("anchored", 0)) == int(coverage.get("expected", 0)),
    "journal_complete": journal.get("complete") is True,
    "discovered_nonempty": int(report.get("discovered_symbols", 0)) > 0,
}
for name, passed in checks.items():
    print(f"{name}: {'PASS' if passed else 'FAIL'}")
sys.exit(0 if all(checks.values()) else 1)
PY
VERDICT=${VERDICT:-pass}
[[ "$LIVEZ_STATUS" == pass ]] || VERDICT=FAIL
[[ "$READYZ_STATUS" == pass ]] || VERDICT=FAIL
[[ "$METRICS_STATUS" == pass ]] || VERDICT=FAIL
[[ "$EXIT_STATUS" == pass ]] || VERDICT=FAIL
[[ "$UNIT_STATUS" == pass ]] || VERDICT=FAIL
[[ "$SBE_STATUS" == pass ]] || VERDICT=FAIL
[[ "$COMPLETE_STATUS" == pass ]] || VERDICT=FAIL
[[ "$COVERAGE_STATUS" == pass ]] || VERDICT=FAIL
[[ "$JOURNAL_STATUS" == pass ]] || VERDICT=FAIL

COMPLETED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
python3 - "$SANDBOX" "$VAR_LIB" "$COMMIT" "$STARTED_AT" "$COMPLETED_AT" \
  "$DURATION_SECS" "$METRICS_PORT" "$LIVEZ_STATUS" "$READYZ_STATUS" "$METRICS_STATUS" \
  "$EXIT_STATUS" "$UNIT_STATUS" "$SBE_STATUS" "$COMPLETE_STATUS" "$COVERAGE_STATUS" \
  "$JOURNAL_STATUS" "$VERDICT" <<'PY'
import hashlib, json, pathlib, sys
(sandbox, var_lib, commit, started, completed, duration, port,
 livez, readyz, metrics, exit_status, unit_status, sbe, complete,
 coverage, journal, verdict) = sys.argv[1:]
root, lib = pathlib.Path(sandbox), pathlib.Path(var_lib)
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
settings = root.parent / "etc" / "engineering-challenge" / "settings.toml"
manifest = {
    "schema_version": 1,
    "verification": "deploy",
    "source_commit": commit,
    "started_at_utc": started,
    "completed_at_utc": completed,
    "duration_secs": int(duration),
    "metrics_port": int(port),
    "layout": {
        "settings": str(settings),
        "settings_sha256": sha(settings),
        "var_lib": str(lib),
    },
    "checks": {
        "livez_200": livez == "pass",
        "readyz_200": readyz == "pass",
        "metrics_scrape": metrics == "pass",
        "clean_exit": exit_status == "pass",
        "unit_hardening": unit_status == "pass",
        "sbe_reconstruction": sbe == "pass",
        "completeness": complete == "pass",
        "coverage": coverage == "pass",
        "journal": journal == "pass",
    },
    "artifacts": {
        name: {"sha256": sha(lib / name)}
        for name in ("capture.sbe", "markets.json", "report.json")
    },
    "verification_reports": {
        name: {"path": name, "sha256": sha(root / f"{name}.json")}
        for name in ("sbe-verify", "completeness", "coverage", "journal-verify")
        if (root / f"{name}.json").is_file()
    },
    "verdict": verdict,
}
(root / "deploy-verification.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
print(json.dumps(manifest, indent=2))
PY

if [[ "$VERDICT" == pass ]]; then
  echo "DEPLOY_VERIFICATION_OK: $SANDBOX"
else
  echo "DEPLOY_VERIFICATION_FAILED: inspect $SANDBOX" >&2
  exit 1
fi

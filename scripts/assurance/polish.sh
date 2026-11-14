#!/usr/bin/env bash
# The polish gate: bring the tree to the state CI expects and prove it.
#
# Usage:
#   scripts/assurance/polish.sh [--loop N]
#
# Iteration 1 applies rustfmt; every iteration re-checks the entire gate.
# --loop N repeats the whole gate N times: repetition exists to surface
# flaky, order-dependent or environment-sensitive behavior. A gate that
# only passes sometimes is a failing gate.
#
# Tree stability is verified inside every iteration: the gate itself must
# not mutate tracked files, so a test that rewrites a fixture or a manifest
# cannot hide behind a passing exit status.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/gate.sh"
gate_cd_root

loops=1
if [[ "${1:-}" == "--loop" ]]; then
  loops="${2:?--loop requires a repetition count}"
fi
if ! [[ "$loops" =~ ^[0-9]+$ ]] || (( loops < 1 )); then
  echo "--loop requires an integer >= 1" >&2
  exit 2
fi

tree_fingerprint() {
  git diff --binary | git hash-object --stdin
}

check_shell_syntax() {
  local status=0 script
  while IFS= read -r -d '' script; do
    if ! bash -n "$script"; then
      printf 'syntax error: %s\n' "$script" >&2
      status=1
    fi
  done < <(find scripts -type f -name '*.sh' -print0)
  return "$status"
}

# ---- phase 0: certify the tools that guard the repository ----------------
run_step "shell syntax (bash -n over scripts/)" check_shell_syntax
run_optional_step "shellcheck over scripts/" shellcheck \
  scripts/run-live-evidence.sh scripts/verify-journal-enospc.sh \
  scripts/assurance/gate.sh scripts/assurance/polish.sh \
  scripts/assurance/update.sh scripts/assurance/upgrade.sh \
  scripts/assurance/run_audit.sh
run_step "python unit tests (scripts)" \
  python3 -m unittest discover -s scripts -p 'test_*.py'
run_step "python unit tests (scripts/assurance)" \
  python3 -m unittest discover -s scripts/assurance -p 'test_*.py'
run_step "requirement-matrix references" python3 scripts/check_matrix_tests.py
run_step "architecture traceability" python3 scripts/check_architecture.py --check
if cargo audit --version >/dev/null 2>&1; then
  run_step "cargo audit" cargo audit
else
  printf '== cargo audit: skipped (cargo-audit is not installed; install it for full coverage)\n'
fi

# ---- phase 1: the CI-parity build-and-verify core, iterated --------------
fingerprint_start="$(tree_fingerprint)"

iteration=1
while (( iteration <= loops )); do
  printf '\n======== polish iteration %d/%d ======== \n' "$iteration" "$loops"
  if (( iteration == 1 )); then
    run_step "cargo fmt (apply)" cargo fmt --all
  fi
  run_step "cargo fmt --check" cargo fmt --all -- --check
  run_step "cargo clippy -D warnings" cargo clippy --all-targets --locked -- -D warnings
  run_step "cargo test (all targets)" cargo test --all-targets --locked
  run_step "cargo test (doc)" cargo test --doc --locked
  run_step "cargo build (release)" cargo build --release --locked
  if [[ "$(uname -s)" == "Linux" ]]; then
    run_step "journal ENOSPC drill" scripts/verify-journal-enospc.sh
  else
    printf '== journal ENOSPC drill: skipped on %s (Linux-only CI step)\n' "$(uname -s)"
  fi
  run_step "english audit (advisory)" scripts/assurance/run_audit.sh

  fingerprint_now="$(tree_fingerprint)"
  if [[ "$fingerprint_now" == "$fingerprint_start" ]]; then
    printf '== tree stability: pass (the gate mutated nothing)\n'
  else
    printf '== tree stability: FAIL (the gate modified tracked files)\n' >&2
    git diff --stat >&2
    GATE_FAILED=1
  fi

  iteration=$((iteration + 1))
done

gate_summary

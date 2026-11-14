#!/usr/bin/env bash
# Shared machinery for the assurance gates (polish, update, upgrade).
#
# Sourced, not executed. Every gate is fail-closed: a step that cannot run
# is a failure, never an implicit pass. A failing step marks the whole gate
# failed, and the remaining steps still run, so one bad step cannot hide
# another.

# shellcheck shell=bash
set -euo pipefail

GATE_ROOT="$(git rev-parse --show-toplevel)"
GATE_FAILED=0
GATE_STEPS=()

gate_cd_root() {
  cd "$GATE_ROOT"
}

# run_step NAME COMMAND [ARG...]
#
# Runs COMMAND, records its status and duration in the gate summary.
run_step() {
  local name="$1"
  shift
  local started status seconds
  started=${SECONDS}
  printf '\n== %s\n' "$name"
  if "$@"; then
    status=pass
  else
    status=FAIL
    GATE_FAILED=1
  fi
  seconds=$((SECONDS - started))
  GATE_STEPS+=("${name}|${status}|${seconds}")
  printf '== %s: %s (%ss)\n' "$name" "$status" "$seconds"
}

# run_optional_step NAME COMMAND [ARG...]
#
# Enforces COMMAND when its tool is installed and records an explicit skip
# when it is not. An unavailable tool is never a hidden pass: the summary
# names it.
run_optional_step() {
  local name="$1"
  shift
  if command -v "$1" >/dev/null 2>&1; then
    run_step "$name" "$@"
  else
    printf '== %s: skipped (%s is not installed; install it for full coverage)\n' \
      "$name" "$1"
  fi
}

gate_summary() {
  printf '\n======== gate summary ========\n'
  local entry name rest status seconds
  for entry in ${GATE_STEPS[@]+"${GATE_STEPS[@]}"}; do
    name="${entry%%|*}"
    rest="${entry#*|}"
    status="${rest%%|*}"
    seconds="${rest##*|}"
    printf '%-56s %-5s %ss\n' "$name" "$status" "$seconds"
  done
  if (( GATE_FAILED != 0 )); then
    printf 'RESULT: FAIL\n'
    return 1
  fi
  printf 'RESULT: PASS\n'
}

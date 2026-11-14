#!/usr/bin/env bash
# Lockfile refresh gate: move every dependency to the newest
# semver-compatible release, then demand the full polish gate before the
# refresh may survive.
#
# Usage:
#   scripts/assurance/update.sh [--commit]
#
# A failed gate restores Cargo.lock automatically: a dependency set that
# cannot prove itself is not adopted. With --commit a passing refresh is
# committed on its own, so it never mixes with feature work.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/gate.sh"
gate_cd_root

lock="Cargo.lock"
if [[ ! -f "$lock" ]]; then
  echo "no $lock; nothing to update" >&2
  exit 2
fi
if ! git diff --quiet -- "$lock"; then
  echo "$lock already has uncommitted changes; commit or restore first" >&2
  exit 2
fi

commit=0
if [[ "${1:-}" == "--commit" ]]; then
  commit=1
fi

run_step "cargo update (lockfile refresh)" cargo update
if (( GATE_FAILED != 0 )); then
  echo "cargo update failed; leaving the tree as-is for inspection" >&2
  exit 1
fi

if git diff --quiet -- "$lock"; then
  echo "lockfile unchanged: already at the newest compatible set"
else
  git diff --stat -- "$lock"
fi

if ! scripts/assurance/polish.sh; then
  printf '\nlockfile refresh failed the gate; restoring %s\n' "$lock"
  git checkout -- "$lock"
  exit 1
fi

if (( commit != 0 )) && ! git diff --quiet -- "$lock"; then
  git add "$lock"
  git commit -m "build: refresh the dependency lockfile (cargo update)"
fi
printf '\nsuggested commit: build: refresh the dependency lockfile (cargo update)\n'

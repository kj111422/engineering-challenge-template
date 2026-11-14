#!/usr/bin/env bash
# Deliberate upgrade gate: move the pinned toolchain forward and every
# dependency to its newest compatible release, then demand the full polish
# gate. A failed upgrade restores the toolchain pins and Cargo.lock.
#
# Usage:
#   scripts/assurance/upgrade.sh [NEW_TOOLCHAIN] [--commit]
#
# Without NEW_TOOLCHAIN this is a dry run: it reports the pinned toolchain
# and exits 0 without touching anything. The CI workflow pins the same
# toolchain and is kept in lockstep automatically.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/gate.sh"
gate_cd_root

toolchain_file="rust-toolchain.toml"
ci_file=".github/workflows/ci.yml"
toolchain=""
commit=0
for arg in "$@"; do
  case "$arg" in
    --commit) commit=1 ;;
    *) toolchain="$arg" ;;
  esac
done

current="$(sed -n 's/^channel *= *"\(.*\)"/\1/p' "$toolchain_file")"
if [[ -z "$current" ]]; then
  echo "cannot read the channel from $toolchain_file" >&2
  exit 2
fi

if [[ -z "$toolchain" ]]; then
  printf 'pinned toolchain: %s\n' "$current"
  printf 'dry run: pass a version (for example %s) to upgrade\n' "$current"
  exit 0
fi

if [[ "$toolchain" == "$current" ]]; then
  echo "already pinned to $toolchain" >&2
  exit 2
fi
if ! git diff --quiet -- "$toolchain_file"; then
  echo "$toolchain_file has uncommitted changes; commit or restore first" >&2
  exit 2
fi

restore_pins() {
  git checkout -- "$toolchain_file" 2>/dev/null || true
  git checkout -- "$ci_file" 2>/dev/null || true
  git checkout -- Cargo.lock 2>/dev/null || true
}
upgraded=0
trap 'if (( upgraded == 0 )); then restore_pins; fi' EXIT

printf 'upgrading toolchain %s -> %s\n' "$current" "$toolchain"
sed -i.bak "s/^channel *= *\".*\"/channel = \"${toolchain}\"/" "$toolchain_file"
rm -f "$toolchain_file.bak"

if command -v rustup >/dev/null 2>&1; then
  run_step "rustup toolchain install" rustup toolchain install "$toolchain" \
    --profile minimal --component rustfmt --component clippy
  if (( GATE_FAILED != 0 )); then
    echo "toolchain installation failed; restoring pins" >&2
    exit 1
  fi
else
  printf 'rustup not found; assuming %s is already installed\n' "$toolchain"
fi

if grep -qE "^[[:space:]]*toolchain: ${current}[[:space:]]*$" "$ci_file"; then
  sed -i.bak \
    "s|^[[:space:]]*toolchain: ${current}[[:space:]]*$|          toolchain: ${toolchain}|" \
    "$ci_file"
  rm -f "$ci_file.bak"
  printf 'updated the CI pin in %s\n' "$ci_file"
else
  printf 'WARNING: no toolchain pin matching %s in %s; update it manually\n' \
    "$current" "$ci_file" >&2
fi

run_step "cargo update (lockfile refresh)" cargo update

if ! scripts/assurance/polish.sh; then
  echo "upgrade failed the gate; restoring pins and lockfile" >&2
  exit 1
fi

if (( commit != 0 )); then
  git add "$toolchain_file" "$ci_file" Cargo.lock
  git commit -m "build: upgrade the toolchain to ${toolchain} and refresh dependencies"
fi
upgraded=1
printf '\nsuggested commit: build: upgrade the toolchain to %s and refresh dependencies\n' "$toolchain"

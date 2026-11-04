#!/usr/bin/env bash
# scripts/quality-gate.sh — run a quality-gate suite and archive results under
# target/quality-runs/<timestamp>/. This is the local replacement for the
# disabled nightly CI workflows; scripts/install-local-gates.sh registers it
# with launchd for unattended runs.
#
# Usage:
#   scripts/quality-gate.sh [nightly|bench|soak|all]
#
# Suites:
#   nightly  gate-invariants + gate-test + gate-crash + gate-fuzz + gate-formal
#   bench    bench-gate (criterion vs the saved "main" baseline)
#   soak     soak-smoke (short seeded soak)
#   all      nightly + bench + soak
#
# Every step runs to completion even if an earlier step failed — a gate run
# should return the full signal, not the first failure. The run exits 1 if
# any step failed. Per-step logs and SUMMARY.md land in the run directory.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
# launchd starts with a minimal environment; make tool lookup deterministic.
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/.cargo/bin:/usr/bin:/bin:/usr/sbin:/sbin"
cd "$ROOT"

suite="${1:-nightly}"
case "$suite" in
  nightly) steps=(gate-invariants gate-test gate-crash gate-fuzz gate-formal) ;;
  bench)   steps=(bench-gate) ;;
  soak)    steps=(soak-smoke) ;;
  all)     steps=(gate-invariants gate-test gate-crash gate-fuzz gate-formal bench-gate soak-smoke) ;;
  *) echo "usage: quality-gate.sh [nightly|bench|soak|all]" >&2; exit 2 ;;
esac

ts="$(date +%Y%m%d-%H%M%S)"
run_dir="$ROOT/target/quality-runs/$ts-$suite"
mkdir -p "$run_dir"
summary="$run_dir/SUMMARY.md"

fail=0
{
  echo "# quality-gate — suite: $suite"
  echo
  echo "- started: $(date -Iseconds)"
  echo "- repo: $ROOT"
  echo "- head: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo
  echo "| step | status | log |"
  echo "|---|---|---|"
} > "$summary"

for step in "${steps[@]}"; do
  log="$run_dir/$step.log"
  echo "[quality-gate] $step ... (log: $log)"
  started=$(date +%s)
  if just "$step" > "$log" 2>&1; then
    status="PASS"
  else
    status="FAIL"
    fail=1
  fi
  elapsed=$(( $(date +%s) - started ))
  echo "| $step | $status (${elapsed}s) | $(basename "$log") |" >> "$summary"
  echo "[quality-gate] $step $status (${elapsed}s)"
done

{
  echo
  if [[ "$fail" -eq 0 ]]; then
    echo "overall: PASS"
  else
    echo "overall: FAIL"
  fi
} >> "$summary"

echo "[quality-gate] summary: $summary"
if [[ "$fail" -eq 0 ]]; then
  echo "[quality-gate] overall: PASS"
else
  echo "[quality-gate] overall: FAIL — inspect $run_dir" >&2
fi
exit "$fail"

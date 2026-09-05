#!/usr/bin/env bash
# scripts/perf-baseline.sh — capture the rr release performance baseline matrix.
#
# Runs rr-perf-bench (release build) across the storage/durability matrix and
# writes one JSON per run plus CPU samples for the rocks/strict S2 sweep
# (bottleneck evidence for ADR A-0010 precondition 2: "is storage the binding
# constraint, or does CPU saturate first?").
#
# Prerequisites:
#   cargo build --release -p rr-perf-bench -p rr-server --features rr-server/rocks -p rr-admin
#
# Usage:
#   scripts/perf-baseline.sh [output-dir]        # default: docs/perf/baselines
#   DURATION_SECS=10 WARMUP_SECS=2 scripts/perf-baseline.sh /tmp/quick  # smoke
#
# Runs are sequential (all use the default port base); keep the machine quiet
# while this runs — compilation or other load on the host contaminates results.
set -euo pipefail

OUT_DIR="${1:-docs/perf/baselines}"
DURATION="${DURATION_SECS:-20}"
WARMUP="${WARMUP_SECS:-5}"
LEVELS="${CONCURRENCY_LEVELS:-1,10,50,100,200,500}"
BIN="${RR_PERF_BENCH_BIN:-target/release/rr-perf-bench}"

cd "$(dirname "$0")/.."

if [[ ! -x "$BIN" ]]; then
    echo "error: $BIN not found or not executable." >&2
    echo "build first: cargo build --release -p rr-perf-bench -p rr-server --features rr-server/rocks -p rr-admin" >&2
    exit 1
fi
mkdir -p "$OUT_DIR"

run() { # <name> <extra rr-perf-bench args...>
    local name="$1"
    shift
    echo "=== baseline run: ${name} ==="
    "$BIN" --warmup-secs "$WARMUP" --duration-secs "$DURATION" \
        --concurrency "$LEVELS" "$@" \
        --json-out "${OUT_DIR}/raw-${name}.json"
    # let the previous run's ports fully settle before the next cluster binds
    sleep 5
}

# Sample rr-server CPU% every 4s for ~3 minutes (covers the S2 sweep).
sample_cpu() {
    local out="$1"
    for _ in $(seq 1 45); do
        ps -Ao pcpu,args | grep '[r]r-server' || true
        sleep 4
    done > "$out"
}

# ADR A-0010 precondition runs (S1 serial + S2 at the documented durability
# levels), plus test_only / mem comparisons so the fsync cost is isolated.
sample_cpu "${OUT_DIR}/cpu-samples-s2-rocks-strict.txt" &
SAMPLER=$!
run s2-rocks-strict --scenario s2 --storage rocks --durability strict
kill "$SAMPLER" 2>/dev/null || true
wait "$SAMPLER" 2>/dev/null || true

# NOTE: rr-perf-bench's --durability uses clap's kebab-case value names
# (strict / replicated-only / test-only); the spawned rr-server receives the
# snake_case form. Do not "fix" one to the other's spelling.
run s1-rocks-strict --scenario s1 --storage rocks --durability strict
run s2-rocks-replicated-only --scenario s2 --storage rocks --durability replicated-only
run s2-rocks-test-only --scenario s2 --storage rocks --durability test-only
run s2-mem --scenario s2 --storage mem

echo "baseline matrix complete -> ${OUT_DIR}"

#!/usr/bin/env bash
# scripts/bench-compare.sh — parse `cargo bench -- --baseline main` output and
# apply the R-7 regression thresholds:
#   statistically significant (p < 0.05) AND mean regression > 15%  -> fail
#   significant and 5–15%                                           -> warning
#   everything else (incl. "no baseline yet")                       -> pass
#
# Usage: cargo bench --workspace --features rr-benches/rocks -- --baseline main | tee bench-output.txt
#        scripts/bench-compare.sh bench-output.txt
#
# Exit codes: 0 = pass (incl. warnings), 1 = regression above the fail
# threshold, 2 = usage error (missing/empty input — a lost bench log must
# fail loudly, never read as "no regressions"). Exit 1 blocks the CI bench
# job, which has been a BLOCKING gate since 2026-09-22 (see the `bench` job
# comment in .github/workflows/ci.yml).
set -euo pipefail

INPUT="${1:?usage: bench-compare.sh <criterion-output-file>}"
if [[ ! -s "$INPUT" ]]; then
    echo "bench-compare: $INPUT missing or empty — no comparison performed" >&2
    exit 2
fi

# Criterion 0.5 prints per benchmark group:
#   <bench-id>                time:   [...]
#                             change: [+lo%, +est%, +hi%] (p = 0.00 < 0.05)
#                             Performance has regressed.
# Only lines with "p = .. < 0.05" are statistically significant.
REPORT=$(awk '
    /^[A-Za-z0-9_\/-]+[[:space:]]+time:/ { bench = $1 }
    /change:/ && /< 0\.05/ {
        line = $0
        gsub(/[][]/, "", line)
        n = split(line, parts, ", ")
        if (n >= 3) {
            est = parts[2]
            gsub(/[%+]/, "", est)
            if (est + 0 > 15)      printf("REGRESS\t%s\t%.2f\n", bench, est)
            else if (est + 0 > 5)  printf("WARN\t%s\t%.2f\n", bench, est)
        }
    }
' "$INPUT")

if [[ -z "$REPORT" ]]; then
    echo "bench-compare: no significant regressions detected (or no baseline data present)"
    exit 0
fi

fail=0
while IFS=$'\t' read -r level bench est; do
    case "$level" in
        REGRESS)
            echo "::error::bench regression: ${bench} +${est}% vs main baseline (p<0.05, threshold 15%)"
            echo "bench-compare: REGRESS ${bench} +${est}%"
            fail=1
            ;;
        WARN)
            echo "::warning::bench regression: ${bench} +${est}% vs main baseline (p<0.05, within 5-15% watch band)"
            echo "bench-compare: WARN    ${bench} +${est}%"
            ;;
    esac
done <<< "$REPORT"

if [[ "$fail" -eq 1 ]]; then
    echo "bench-compare: FAIL — regression(s) above 15% detected"
    exit 1
fi
echo "bench-compare: OK — only sub-15% movements"

#!/usr/bin/env bash
# Chaos: +40ms (±10ms jitter) latency on all inter-node links under load.
# (Calibrated: default election timeout floor is 150ms — link latency beyond
# that guarantees re-election churn; 200ms flaps by design, 40ms must not.)
# Expect: degraded throughput but zero errors and NO leader flapping
# (term must not ratchet repeatedly). Removing the toxic restores baseline.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

preflight
term_before() { admin "$LEADER" --output json group status --group 1 | json_field "['current_term']"; }
t0=$(term_before)

add_latency_all 40 10
start_bench_load 30 50
wait_bench_done 60 || true

LEADER2=$(wait_leader 10) || fail "lost leader under latency"
t1=$(admin "$LEADER2" --output json group status --group 1 | json_field "['current_term']")
[[ $((t1 - t0)) -le 1 ]] || fail "leader flapping under latency (term $t0 -> $t1)"
assert_write_read_leader 20 >/dev/null

toxics_clear_all
sleep 5
assert_write_read_leader 15 >/dev/null
errs=$(bench_errors); [[ "$errs" == "0" ]] || fail "bench saw $errs error row(s) under latency toxic"
pass "latency-spike: 40ms links tolerated, term stable ($t0->$t1), zero errors"

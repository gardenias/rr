#!/usr/bin/env bash
# Chaos: 5% effective packet drop on all inter-node links (timeout toxic)
# under load. Expect: slower replication, zero client errors, no flap storm.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

preflight
t0=$(admin "$LEADER" --output json group status --group 1 | json_field "['current_term']")

add_packet_loss_all 0.05
start_bench_load 30 50
wait_bench_done 60 || true

LEADER2=$(wait_leader 10) || fail "lost leader under packet loss"
t1=$(admin "$LEADER2" --output json group status --group 1 | json_field "['current_term']")
[[ $((t1 - t0)) -le 1 ]] || fail "leader flapping under packet loss (term $t0 -> $t1)"
assert_write_read_leader 20 >/dev/null

toxics_clear_all
sleep 5
wait_catchup 60 >/dev/null || fail "followers did not converge after loss removed"
errs=$(bench_errors); [[ "$errs" == "0" ]] || fail "bench saw $errs error row(s) under packet loss"
pass "packet-loss: 5% drop tolerated, term stable ($t0->$t1), zero errors"

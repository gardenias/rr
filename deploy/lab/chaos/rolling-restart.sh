#!/usr/bin/env bash
# Chaos: rolling restart of all three nodes under continuous load.
# Expect: writes keep flowing throughout; cluster fully consistent at the end.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

preflight
start_bench_load 90 50

for n in 1 2 3; do
  log "restarting kv-$n…"
  docker restart "kv-$n" >/dev/null
  # wait for the node to be back and the cluster to have a working leader
  L=$(wait_leader 20) || fail "no leader while restarting kv-$n"
  assert_write_read_leader 20 >/dev/null
  sleep 12
done

wait_catchup 90 >/dev/null || fail "cluster not consistent after rolling restart"
assert_write_read_leader 20 >/dev/null

wait_bench_done 120 || true
errs=$(bench_errors); [[ "$errs" == "0" ]] || fail "bench saw $errs error row(s) during rolling restart"
pass "rolling-restart: all nodes restarted under load, zero errors, consistent"

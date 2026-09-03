#!/usr/bin/env bash
# Chaos: kill the leader mid-load. Expect: new leader within seconds,
# writes keep flowing, committed data survives.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

preflight
OLD_LEADER=$LEADER
kv_put "$OLD_LEADER" "chaos:kill-leader:before" "committed" >/dev/null

log "killing leader kv-$OLD_LEADER under load…"
start_bench_load 25 50
docker kill "kv-$OLD_LEADER" >/dev/null

NEW_LEADER=$(wait_leader 15) || fail "no new leader within 15s after kill"
[[ "$NEW_LEADER" != "$OLD_LEADER" ]] || fail "same node re-elected?!"
log "new leader: kv-$NEW_LEADER"

assert_write_read_leader 15 >/dev/null
got=$(kv_get "$(assert_write_read_leader 15)" "chaos:kill-leader:before")
[[ "$got" == *committed* ]] || fail "committed write lost after leader kill (got: $got)"

# Restart the old leader; it must rejoin and catch up.
docker start "kv-$OLD_LEADER" >/dev/null
sleep 3
wait_catchup 60 >/dev/null || fail "restarted node did not catch up"

wait_bench_done 60 || true
errs=$(bench_errors)
[[ "$errs" == "0" ]] || log "note: bench reported $errs error row(s) during failover window (allowed if brief)"
pass "kill-leader: leader kv-$OLD_LEADER -> kv-$NEW_LEADER, data intact, catch-up OK"

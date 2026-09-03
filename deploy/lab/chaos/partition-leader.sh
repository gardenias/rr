#!/usr/bin/env bash
# Chaos: network-partition the LEADER (docker network disconnect).
# Expect: minority loses leadership; majority elects a new leader and keeps
# serving; after heal the old leader rejoins with no split-brain (single
# leader, consistent log index, committed data intact).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

preflight
OLD=$LEADER
kv_put "$OLD" "chaos:partition:before" "committed" >/dev/null

log "partitioning leader kv-$OLD (network disconnect $LAB_NET)…"
start_bench_load 30 50
docker network disconnect "$LAB_NET" "kv-$OLD"
sleep 8 # a few election cycles

NEW_LEADER=$(wait_leader_excluding 20 "$OLD") || fail "no leader among majority after partition"
[[ "$NEW_LEADER" != "$OLD" ]] || fail "partitioned node still leader"
assert_write_read_leader 15 "$OLD" >/dev/null

# Old leader must NOT serve writes while partitioned (probe from host fails or no-commits).
before_idx=$(last_log_index "$NEW_LEADER")
if kv_put "$OLD" "chaos:partition:minority" "should-not-commit" 2>&1 | grep -q OK; then
  sleep 2
  [[ "$(last_log_index "$NEW_LEADER")" == "$before_idx" ]] \
    || fail "minority side committed a write during partition"
fi

log "healing partition…"
docker network connect "$LAB_NET" "kv-$OLD"
sleep 3
wait_catchup 180 >/dev/null || fail "old leader did not converge after heal"
FINAL_LEADER=$(assert_write_read_leader 15) || fail "no leader after heal"
got=$(kv_get "$FINAL_LEADER" "chaos:partition:before")
[[ "$got" == *committed* ]] || fail "committed data lost across partition"

# single leader check
count=0; for n in 1 2 3; do
  if admin "$n" --output json group status --group 1 2>/dev/null \
      | json_field "['is_leader']" | grep -q True; then
    count=$((count+1))
  fi
done
[[ "$count" == "1" ]] || fail "split-brain: $count leaders after heal"

wait_bench_done 60 || true
errs=$(bench_errors); [[ "$errs" == "0" ]] || log "note: $errs bench error row(s) during partition window"
pass "partition-leader: kv-$OLD isolated, kv-$NEW_LEADER took over, healed with single leader"

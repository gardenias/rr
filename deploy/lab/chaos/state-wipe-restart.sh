#!/usr/bin/env bash
# Chaos: stop a follower, wipe its data, restart — full state loss recovery.
#
# Validated operator flow (see docs/docs/operations/local-lab.md):
#   1. remove the wiped node from voters (change-membership)
#   2. re-add it as a learner (add-learner)
#   3. wait for catch-up; if the leader's stale progress tracker wedges
#      replication to the re-added node, restart the current leader
#      (a leadership change rebuilds the tracker) — KNOWN LIMITATION, see below
#   4. promote back to voter (change-membership)
#
# KNOWN LIMITATION (found by this lab): with openraft 0.10.0-alpha.34, a wiped
# voter rejoining with the same node id does not get driven to catch-up by the
# incumbent leader (its per-target replication progress retains a stale
# `matching` index). A leadership change (e.g. restarting the leader) rebuilds
# the tracker and catch-up proceeds immediately. This script encodes that
# workaround and asserts it.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

preflight
VICTIM=3; [[ "$LEADER" == "3" ]] && VICTIM=2
kv_put "$LEADER" "chaos:wipe:marker" "must-survive" >/dev/null
OTHERS=""
for n in 1 2 3; do
  if [[ "$n" != "$VICTIM" ]]; then OTHERS+="${n},"; fi
done
OTHERS=${OTHERS%,}

log "stopping kv-$VICTIM and wiping its state…"
docker compose -f "$COMPOSE_BASE" -f "$COMPOSE_CHAOS" stop "kv-$VICTIM" >/dev/null
docker compose -f "$COMPOSE_BASE" -f "$COMPOSE_CHAOS" rm -f "kv-$VICTIM" >/dev/null

# Membership surgery on the leader: victim out of voters, back as learner.
admin "$LEADER" group change-membership --group 1 --voters "$OTHERS" >/dev/null \
  || fail "change-membership $OTHERS failed"
admin "$LEADER" group add-learner --group 1 --node-id "$VICTIM" \
  --addr "toxiproxy:1510$VICTIM" >/dev/null || fail "add-learner failed"

log "restarting kv-$VICTIM from empty state…"
docker compose -f "$COMPOSE_BASE" -f "$COMPOSE_CHAOS" up -d "kv-$VICTIM" >/dev/null
sleep 5

# Majority keeps serving with the victim gone.
assert_write_read_leader 15 "$VICTIM" >/dev/null

# Catch-up watch with the documented leader-restart fallback.
victim_lli() { last_log_index "$VICTIM"; }
leader_lli()  { last_log_index "$(leader_id || echo 1)"; }
if ! catchup_one "$VICTIM" 45; then
  log "catch-up stalled (known stale-progress limitation) — restarting leader…"
  L=$(leader_id)
  docker restart "kv-$L" >/dev/null
  sleep 8
  assert_write_read_leader 20 "$VICTIM" >/dev/null
  catchup_one "$VICTIM" 90 || fail "wiped node did not catch up even after leader restart"
fi

# Promote back to full voter.
L=$(leader_id)
admin "$L" group change-membership --group 1 --voters 1,2,3 >/dev/null \
  || fail "promote-back change-membership failed"
sleep 3
wait_catchup 60 >/dev/null || fail "cluster not consistent after promote-back"

got=$(kv_get "$(assert_write_read_leader 15)" "chaos:wipe:marker")
[[ "$got" == *must-survive* ]] || fail "marker lost during wipe/restart"
pass "state-wipe-restart: kv-$VICTIM rejoined from empty via learner flow, caught up to $(victim_lli), data consistent"

#!/usr/bin/env bash
# Chaos: freeze a follower (docker pause) under load, then resume.
# Expect: cluster keeps serving; follower catches up after unpause.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

preflight
FOLLOWER=""
for n in 1 2 3; do [[ "$n" != "$LEADER" ]] && { FOLLOWER=$n; break; }; done
[[ -n "$FOLLOWER" ]] || fail "no follower found"
log "pausing follower kv-$FOLLOWER for 30s under load…"

start_bench_load 45 50
docker pause "kv-$FOLLOWER" >/dev/null
sleep 30

assert_write_read_leader 15 >/dev/null  # majority intact: writes still work

docker unpause "kv-$FOLLOWER" >/dev/null
wait_catchup 60 >/dev/null || fail "paused follower did not catch up after unpause"
log "follower kv-$FOLLOWER caught up to $(last_log_index "$FOLLOWER")"

wait_bench_done 60 || true
errs=$(bench_errors)
[[ "$errs" == "0" ]] || fail "bench saw $errs error row(s) during follower pause"
pass "pause-follower: kv-$FOLLOWER frozen 30s, zero write errors, catch-up OK"

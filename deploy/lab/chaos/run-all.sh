#!/usr/bin/env bash
# Run the full chaos suite. Each scenario gets a FRESH stack (deterministic
# starting state: small log, clean network, no leftover toxics/load).
#
# Assumes nothing; brings the stack up itself in chaos mode.
set -euo pipefail
cd "$(dirname "$0")/../../.." # repo root

COMPOSE="docker compose -f deploy/lab/docker-compose.yml -f deploy/lab/docker-compose.chaos.yml"

suite=(kill-leader pause-follower partition-leader latency-spike packet-loss state-wipe-restart rolling-restart)
failed=()

fresh_stack() {
  echo "---- resetting stack ----"
  docker rm -f rr-lab-load >/dev/null 2>&1 || true
  $COMPOSE down -v >/dev/null 2>&1 || true
  $COMPOSE up -d >/dev/null
  # wait for all three nodes healthy + a leader
  for _ in $(seq 1 60); do
    ok=0
    for n in 1 2 3; do
      [[ "$(docker inspect -f '{{.State.Health.Status}}' "kv-$n" 2>/dev/null)" == "healthy" ]] && ok=$((ok+1))
    done
    if (( ok == 3 )); then
      if docker exec kv-1 rr-admin --addr 127.0.0.1:52001 --output json group status --group 1 2>/dev/null | grep -q '"is_leader": *true' \
      || docker exec kv-2 rr-admin --addr 127.0.0.1:52001 --output json group status --group 1 2>/dev/null | grep -q '"is_leader": *true' \
      || docker exec kv-3 rr-admin --addr 127.0.0.1:52001 --output json group status --group 1 2>/dev/null | grep -q '"is_leader": *true'; then
        return 0
      fi
    fi
    sleep 1
  done
  echo "STACK FAILED TO BECOME HEALTHY"; return 1
}

for s in "${suite[@]}"; do
  echo "================ $s ================"
  if ! fresh_stack; then failed+=("$s (stack)"); continue; fi
  if bash "deploy/lab/chaos/$s.sh"; then
    echo "================ $s OK ================"
  else
    failed+=("$s")
    echo "================ $s FAILED ================"
  fi
done

if ((${#failed[@]})); then
  printf 'CHAOS SUITE FAILED: %s\n' "${failed[*]}"; exit 1
fi
echo "CHAOS SUITE: all ${#suite[@]} scenarios passed"

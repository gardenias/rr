#!/usr/bin/env bash
# Shared helpers for the rr lab chaos suite.
# shellcheck shell=bash
set -euo pipefail

COMPOSE_BASE=${COMPOSE_BASE:-deploy/lab/docker-compose.yml}
COMPOSE_CHAOS=${COMPOSE_CHAOS:-deploy/lab/docker-compose.chaos.yml}
TOXI_API=${TOXI_API:-http://127.0.0.1:18474}
LAB_NET=${LAB_NET:-rrlab_rrlab}

if [[ -n "${RR_ADMIN_BIN:-}" ]]; then
  :
elif [[ -x target/release/rr-admin ]]; then
  RR_ADMIN_BIN=target/release/rr-admin
else
  RR_ADMIN_BIN=target/debug/rr-admin
fi

# Every sourced script gets a composable cleanup: stop the load generator and
# clear all toxics, whatever the exit path.
_rr_lab_cleanup() {
  docker rm -f rr-lab-load >/dev/null 2>&1 || true
  toxics_clear_all >/dev/null 2>&1 || true
}
trap _rr_lab_cleanup EXIT

log()  { printf '[chaos %s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "FAIL: $*"; exit 1; }
pass() { log "PASS: $*"; }

# docker exec into a kv node's admin CLI (admin port is fixed 52001 in-image).
admin() { # admin <node-idx> <args...>
  local n=$1; shift
  docker exec "kv-$n" rr-admin --addr 127.0.0.1:52001 "$@"
}

json_field() { python3 -c "
import json,sys
try:
    d=json.load(sys.stdin); print(d$1)
except Exception:
    print('')"; }

# Current leader node id for group 1 (empty if none).
leader_id() {
  local n
  for n in 1 2 3; do
    if admin "$n" --output json group status --group 1 2>/dev/null \
        | json_field "['is_leader']" | grep -q True; then
      echo "$n"; return 0
    fi
  done
  echo ""; return 1
}

wait_leader() { # wait_leader <timeout-secs> — prints leader id
  local deadline=$(( $(date +%s) + $1 ))
  while (( $(date +%s) < deadline )); do
    local id; id=$(leader_id || true)
    [[ -n "$id" ]] && { echo "$id"; return 0; }
    sleep 0.5
  done
  return 1
}

last_log_index() { # last_log_index <node-idx> — empty when unreachable
  admin "$1" --output json group status --group 1 2>/dev/null | json_field "['last_log_index']" || true
}

wait_catchup() { # wait_catchup <timeout-secs> — all three nodes equal (non-empty) last_log_index
  local deadline=$(( $(date +%s) + $1 ))
  while (( $(date +%s) < deadline )); do
    local a b c
    a=$(last_log_index 1); b=$(last_log_index 2); c=$(last_log_index 3)
    [[ -n "$a" && "$a" == "$b" && "$b" == "$c" ]] && { echo "$a"; return 0; }
    sleep 1
  done
  return 1
}

host_port() { echo "5110$1"; } # client port published for kv-<idx>

# KV ops against a specific node via the host-published port (repl speaks
# bincode KvRequest; writes must target the leader).
kv_put() { # kv_put <node-idx> <key> <value>
  printf 'put %s %s\n' "$2" "$3" \
    | "$RR_ADMIN_BIN" repl --addr "127.0.0.1:$(host_port "$1")" --group 1 2>&1
}
kv_get() { # kv_get <node-idx> <key> — prints raw repl answer
  printf 'get %s\n' "$2" \
    | "$RR_ADMIN_BIN" repl --addr "127.0.0.1:$(host_port "$1")" --group 1 2>&1
}

# --- assertions ---------------------------------------------------------------

# Single-node catch-up: one node's last_log_index reaches the leader's.
catchup_one() { # catchup_one <node-idx> <timeout-secs>
  local victim=$1 deadline=$(( $(date +%s) + $2 ))
  while (( $(date +%s) < deadline )); do
    local v l
    v=$(victim_lli); l=$(leader_lli)
    [[ -n "$v" && -n "$l" && "$v" == "$l" ]] && return 0
    sleep 1
  done
  return 1
}

assert_write_read() { # assert_write_read <node-idx> [retries] — probe with retries:
  # right after an election a node can report is_leader before it serves writes.
  local n=$1 tries=${2:-10} k="chaos:probe:$RANDOM" v="v$RANDOM" got
  for _ in $(seq 1 "$tries"); do
    if kv_put "$n" "$k" "$v" 2>/dev/null | grep -q OK; then
      got=$(kv_get "$n" "$k")
      [[ "$got" == *"$v"* ]] && return 0
    fi
    sleep 1
  done
  fail "write/read probe failed on kv-$n after ${tries} tries"
}

# Probe whoever is leader RIGHT NOW — leadership moves around during chaos,
# so a fixed-node probe is flaky by construction. An optional second arg
# excludes a node (e.g. the partitioned one, which still self-reports as
# leader to docker-exec admin queries and whose published port is dead).
assert_write_read_leader() { # assert_write_read_leader [retries] [exclude-id]
  local tries=${1:-15} excl=${2:-} i id
  for i in $(seq 1 "$tries"); do
    id=""
    for n in 1 2 3; do
      [[ -n "$excl" && "$n" == "$excl" ]] && continue
      if admin "$n" --output json group status --group 1 2>/dev/null \
          | json_field "['is_leader']" | grep -q True; then
        id=$n; break
      fi
    done
    if [[ -n "$id" ]] && assert_write_read "$id" 2 2>/dev/null; then
      echo "$id"; return 0
    fi
    sleep 1
  done
  fail "no leader served a write/read probe within ${tries}s"
}

# Leader among the nodes EXCLUDING one (e.g. the partitioned node, which still
# self-reports as leader to docker-exec admin queries).
wait_leader_excluding() { # wait_leader_excluding <timeout-secs> <excluded-id>
  local deadline=$(( $(date +%s) + $1 )) excl=$2 id
  while (( $(date +%s) < deadline )); do
    for id in 1 2 3; do
      [[ "$id" == "$excl" ]] && continue
      if admin "$id" --output json group status --group 1 2>/dev/null \
          | json_field "['is_leader']" | grep -q True; then
        echo "$id"; return 0
      fi
    done
    sleep 0.5
  done
  return 1
}

# Counter from the Kafka producer path must never go backwards and must still
# be increasing after heal (when the producer is running).
counter_value() { # counter_value <node-idx> — integer or empty
  kv_get "$1" "lab:counter" | grep -oE '[0-9]+' | tail -1 || true
}

# --- toxiproxy ----------------------------------------------------------------

toxi() { curl -fsS -X "$1" "$TOXI_API$2" ${3:+-d "$3"} -H 'Content-Type: application/json'; }

toxics_clear_all() {
  local p
  for p in kv1 kv2 kv3; do
    curl -fsS "$TOXI_API/proxies/$p/toxics" \
      | python3 -c 'import json,sys; [print(t["name"]) for t in json.load(sys.stdin)]' \
      | while read -r t; do curl -fsS -X DELETE "$TOXI_API/proxies/$p/toxics/$t" >/dev/null; done
  done
}

add_latency_all() { # add_latency_all <ms> <jitter-ms>
  local p
  for p in kv1 kv2 kv3; do
    toxi POST "/proxies/$p/toxics" \
      "{\"name\":\"latency-$p\",\"type\":\"latency\",\"stream\":\"upstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$1,\"jitter\":$2}}" >/dev/null
  done
  log "injected latency=${1}ms jitter=${2}ms on all inter-node links"
}

add_packet_loss_all() { # add_packet_loss_all <percent> — via timeout toxic prob p (slicer is rate-based; timeout+p approximates loss)
  local p
  for p in kv1 kv2 kv3; do
    toxi POST "/proxies/$p/toxics" \
      "{\"name\":\"loss-$p\",\"type\":\"timeout\",\"stream\":\"upstream\",\"toxicity\":$1,\"attributes\":{\"timeout\":0}}" >/dev/null
  done
  log "injected packet-drop probability=$1 on all inter-node links"
}

# --- load generator -----------------------------------------------------------

start_bench_load() { # start_bench_load <duration-secs> [concurrency]
  local dur=$1 conc=${2:-50}
  docker rm -f rr-lab-load >/dev/null 2>&1 || true # clear any stale container from a previous scenario
  docker compose -f "$COMPOSE_BASE" -f "$COMPOSE_CHAOS" --profile bench run -d \
    --name rr-lab-load bench \
    --cluster-addrs=kv-1:51001,kv-2:51001,kv-3:51001 \
    --scenario=s2 --concurrency "$conc" --warmup-secs=2 --duration-secs="$dur" >/dev/null
  log "bench load started (s2 c=$conc ${dur}s, container rr-lab-load)"
}

wait_bench_done() { # wait_bench_done <timeout-secs> — wait until the load container exits (not --rm; logs stay inspectable)
  local deadline=$(( $(date +%s) + $1 ))
  while (( $(date +%s) < deadline )); do
    [[ -z "$(docker ps -q -f name=rr-lab-load -f status=running)" ]] && return 0
    sleep 2
  done
  return 1
}

bench_errors() { # non-zero error rows in the load container's S2 table
  docker logs rr-lab-load 2>/dev/null \
    | grep -E '^  [0-9]+ +[0-9]' | awk '{if ($NF != "0") bad++} END {print bad+0}'
}

preflight() {
  log "preflight: waiting for leader…"
  LEADER=$(wait_leader 30) || fail "no leader at preflight"
  log "preflight: leader=kv-$LEADER; write/read probe…"
  assert_write_read "$LEADER"
  wait_catchup 30 >/dev/null || fail "cluster not caught up at preflight"
  log "preflight OK"
}

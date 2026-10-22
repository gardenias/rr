#!/usr/bin/env bash
# prod-perf.sh — production-shape benchmark matrix runner (rr-prod-perf plan
# §5.3, milestone M3). Runs W1–W9 + W11 against deploy/perf/ (secure stack +
# overlays) and archives machine-readable artifacts + a rendered report to
# docs/perf/prod/<date>-<sha>/.
#
# Usage (repo root):
#   scripts/prod-perf.sh                  # full matrix (~40 min + 2h soak)
#   scripts/prod-perf.sh --smoke          # every tier shrunk (self-validation, ~6 min)
#   WORKLOADS=w1,w8 scripts/prod-perf.sh  # subset
#   scripts/prod-perf.sh --keep           # leave the stack running at the end
#
# Hard rules (plan §5.3/§5.4): this script is NEW and never calls or modifies
# scripts/perf-baseline.sh, scripts/soak.sh, deploy/lab/, or the CI bench job.
# Every tier emits raw JSON (+ 1s time series) with full config echo; report.md
# is RENDERED from the raw files by scripts/prod-perf-report.py — numbers in
# the report are never hand-copied.
#
# Tier env knobs (defaults in parentheses; --smoke shrinks durations and
# truncates ladders to their first three rungs):
#   W1_RATES (100,500,1000,2000,4000,8000,16000,32000)  W1_RUNG_SECS (30)
#   W1_PAYLOADS ("128 1024 4096")   W5_PAYLOADS ("128 1024 4096 16384")
#   W2_RATE (3000)  W3_RATE (6000)  W4_RATE (3000)  W5_RATE (3000)
#   W2_W5_SECS (30)                 W4_KEY_SPACE (100000)
#   W6_PRODUCER_RATE (2000)  W6_SECS (60)
#   W7_RATES (100,500,1000,2000,4000,8000)  W7_SNAPSHOT (5000)
#   W8_RATES (= W1_RATES)           W11_RATES (= W7_RATES)
#   W9_RATE (2000)  W9_SECS (7200)  W9_TRANSFER_AT_SECS (half of W9_SECS)
set -euo pipefail
cd "$(dirname "$0")/.."

# ── args / knobs ─────────────────────────────────────────────────────────────
SMOKE=0
KEEP=0
WORKLOADS=${WORKLOADS:-sec,w1,w2,w3,w4,w5,w6,w7,w8,w9,w11}
for arg in "$@"; do
  case "$arg" in
    --smoke) SMOKE=1 ;;
    --keep) KEEP=1 ;;
    *) echo "unknown arg: $arg (expect --smoke | --keep)" >&2; exit 2 ;;
  esac
done

W1_RATES=${W1_RATES:-100,500,1000,2000,4000,8000,16000,32000}
W1_RUNG_SECS=${W1_RUNG_SECS:-30}
W1_PAYLOADS=${W1_PAYLOADS:-"128 1024 4096"}
W2_RATE=${W2_RATE:-3000}
W3_RATE=${W3_RATE:-6000}
W4_RATE=${W4_RATE:-3000}
W5_RATE=${W5_RATE:-3000}
W2_W5_SECS=${W2_W5_SECS:-30}
W4_KEY_SPACE=${W4_KEY_SPACE:-100000}
W5_PAYLOADS=${W5_PAYLOADS:-"128 1024 4096 16384"}
W6_PRODUCER_RATE=${W6_PRODUCER_RATE:-2000}
W6_SECS=${W6_SECS:-60}
W7_RATES=${W7_RATES:-100,500,1000,2000,4000,8000}
W7_SNAPSHOT=${W7_SNAPSHOT:-5000}
W8_RATES=${W8_RATES:-$W1_RATES}
W11_RATES=${W11_RATES:-$W7_RATES}
W9_RATE=${W9_RATE:-2000}
W9_SECS=${W9_SECS:-7200}
W9_TRANSFER_AT_SECS=${W9_TRANSFER_AT_SECS:-$((W9_SECS / 2))}

if [[ $SMOKE == 1 ]]; then
  W1_RUNG_SECS=10
  W1_RATES=$(printf '%s' "$W1_RATES" | cut -d, -f1-3)
  W8_RATES=$(printf '%s' "$W8_RATES" | cut -d, -f1-3)
  W7_RATES=$(printf '%s' "$W7_RATES" | cut -d, -f1-3)
  W11_RATES=$(printf '%s' "$W11_RATES" | cut -d, -f1-3)
  W2_W5_SECS=10
  W6_SECS=20
  W9_SECS=60
  W9_TRANSFER_AT_SECS=30
fi

# ── constants / paths ────────────────────────────────────────────────────────
PERF_DIR=deploy/perf
BASE_YML=$PERF_DIR/docker-compose.yml
KAFKA_YML=$PERF_DIR/docker-compose.kafka.yml
INSECURE_YML=$PERF_DIR/docker-compose.insecure.yml
MEMLOG_YML=$PERF_DIR/docker-compose.memlog.yml
GIT_SHA=$(git rev-parse --short HEAD)
TS=$(date +%Y%m%d-%H%M%S)
OUT=docs/perf/prod/$TS-$GIT_SHA
PROM_HOST=${PROM_HOST:-http://127.0.0.1:9190}
PROM_INNET=http://prometheus:9090
ADDRS="kv-1:51001,kv-2:51001,kv-3:51001"
TLS_ARGS=(--tls-ca=/etc/rr/certs/ca.pem --tls-cert=/etc/rr/certs/client.pem --tls-key=/etc/rr/certs/client.key --token-env=RR_CLIENT_TOKEN)

mkdir -p "$OUT"
# The bench container (uid 10001) writes into the bind-mounted OUT dir; on
# Linux hosts a root-created dir would deny it (macOS virtiofs is lenient).
chmod 0777 "$OUT"

log() { printf '[prod-perf %s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "FAIL: $*"; exit 1; }

# Per-tier status ledger: "<tier> <ok|fail> <note>" — the report renders red
# rows from it, and the script exits non-zero if any tier failed.
LEDGER="$OUT/ledger.txt"
: >"$LEDGER"

COMPOSE=()
compose_files() { COMPOSE=(docker compose -f "$BASE_YML" "$@"); }

wait_healthy() { # wait_healthy <container> <tries>
  local c=$1 tries=$2
  for _ in $(seq 1 "$tries"); do
    [[ $(docker inspect -f '{{.State.Health.Status}}' "$c" 2>/dev/null) == "healthy" ]] && return 0
    sleep 2
  done
  return 1
}

# Stream node logs to the archive from container start. OrbStack's docker log
# buffer rotates under load and destroys panic origins (M4 run 20260912:
# RaftCore panicked and the line was gone from `docker logs` by the time the
# forensics dump ran). The -f stream is immune to that rotation.
LOGPIDS="$OUT/.logpids"

stop_log_streams() {
  [[ -f $LOGPIDS ]] || return 0
  while read -r pid; do kill "$pid" 2>/dev/null || true; done <"$LOGPIDS"
  rm -f "$LOGPIDS"
}

start_log_streams() {
  stop_log_streams
  local n
  for n in 1 2 3; do
    docker logs -f "perf-kv-$n" >>"$OUT/node$n-stream.log" 2>&1 &
    echo $! >>"$LOGPIDS"
  done
}

stack_up() { # stack_up <label> [extra -f overlay args…]
  local label=$1; shift
  compose_files "$@"
  log "bringing up $label stack…"
  docker compose -f "$BASE_YML" -f "$KAFKA_YML" -f "$INSECURE_YML" -f "$MEMLOG_YML" down -v >/dev/null 2>&1 || true
  # No --build here: the image is built once in preflight. (Re-building per
  # phase re-resolves base-image manifests over the network; this lab is
  # local-images-only and the network check is flaky.)
  "${COMPOSE[@]}" up -d >/dev/null
  local n
  for n in 1 2 3; do
    wait_healthy "perf-kv-$n" 60 || { docker logs "perf-kv-$n" 2>&1 | tail -20; fail "perf-kv-$n never healthy ($label)"; }
  done
  log "$label stack ready"
  start_log_streams
}

stack_down() {
  stop_log_streams
  [[ $KEEP == 1 ]] && return 0
  docker compose -f "$BASE_YML" -f "$KAFKA_YML" -f "$INSECURE_YML" -f "$MEMLOG_YML" down -v >/dev/null 2>&1 || true
}
trap stack_down EXIT

# run_bench <tier-name> <extra rr-perf-bench args…> — secure leg (mTLS+token).
# The container writes raw JSON (+ optional time series) into /out.
run_bench() {
  local name=$1; shift
  "${COMPOSE[@]}" --profile bench run --rm \
    -v "$PWD/$OUT:/out" \
    bench \
    "--cluster-addrs=$ADDRS" \
    "${TLS_ARGS[@]}" \
    "--prom-url=$PROM_INNET" \
    "--json-out=/out/raw-$name.json" \
    "$@" >>"$OUT/bench-$name.log" 2>&1
}

# run_bench_plain <args…> — W8 insecure leg (no TLS/token on the wire).
run_bench_plain() {
  local name=$1; shift
  "${COMPOSE[@]}" --profile bench run --rm \
    -v "$PWD/$OUT:/out" \
    bench \
    "--cluster-addrs=$ADDRS" \
    "--prom-url=$PROM_INNET" \
    "--json-out=/out/raw-$name.json" \
    "$@" >>"$OUT/bench-$name.log" 2>&1
}

# json_field <file> <python-expr-on-r> — tiny read helper.
json_field() { python3 -c "import json; r=json.load(open('$1')); print($2)"; }

# Post-tier writability gate (M4 lesson, run 20260912-000736): the bench
# process exits 0 even when every op errored, so a dead cluster used to
# ledger as "ok" and poison every later tier (2h wasted on a corpse).
# After each tier the cluster MUST still accept writes; if it cannot, the
# tier is ledgered as failed and the matrix aborts — later numbers on a
# dead cluster are garbage. Skipped for `sec` (plaintext is rejected by
# design) and `w8` (the plaintext stack would reject this TLS probe).
# Post-liveness-fix (9fa8347) the cluster legitimately needs ~30s to
# re-stabilize after an extreme-bandwidth tier, so probe 3× over ~40s
# before declaring it dead.
health_gate() {
  local attempt ok
  for attempt in 1 2 3; do
    "${COMPOSE[@]}" --profile bench run --rm \
      -v "$PWD/$OUT:/out" \
      bench \
      "--cluster-addrs=$ADDRS" \
      "${TLS_ARGS[@]}" \
      "--json-out=/out/raw-health.json" \
      --scenario=mix --rate=20 --duration-secs=5 --warmup-secs=1 --mix=write:100 \
      >>"$OUT/bench-health.log" 2>&1 || true
    ok=$(json_field "$OUT/raw-health.json" 'r["mix"]["runs"][0]["ops"][0]["ok"]' 2>/dev/null) || ok=0
    [[ $ok -gt 0 ]] && return 0
    [[ $attempt -lt 3 ]] && sleep 20
  done
  return 1
}

# tier <id> <note> <cmd…> — run one tier, ledger the outcome, never abort the
# matrix on a single-tier failure.
tier() {
  local id=$1 note=$2
  shift 2
  if ! printf ',%s,' "$WORKLOADS" | grep -q ",$id,"; then
    log "[$id] skipped (not in WORKLOADS)"
    return 0
  fi
  log "[$id] $note — start"
  local rc=0
  "$@" || rc=$?
  if [[ $rc == 0 && $id != sec && $id != w8 ]] && ! health_gate; then
    log "[$id] bench exited 0 but cluster is UNWRITABLE — aborting matrix"
    printf '%s fail %s (post-tier health gate)\n' "$id" "$note" >>"$LEDGER"
    fail "cluster unwritable after tier $id"
  fi
  if [[ $rc == 0 ]]; then
    log "[$id] OK"
    printf '%s ok %s\n' "$id" "$note" >>"$LEDGER"
  else
    log "[$id] FAILED (rc=$rc) — continuing with the matrix"
    printf '%s fail %s\n' "$id" "$note" >>"$LEDGER"
  fi
}

# ── tier implementations ─────────────────────────────────────────────────────

# Security negative control (plan acceptance #1): a plaintext client MUST be
# rejected — every op errors, zero succeed.
sec_tier() {
  run_bench_plain sec-neg --scenario=mix --rate=50 --duration-secs=5 --warmup-secs=2
  local ok err
  ok=$(json_field "$OUT/raw-sec-neg.json" 'r["mix"]["runs"][0]["ops"][0]["ok"]')
  err=$(json_field "$OUT/raw-sec-neg.json" 'r["mix"]["runs"][0]["ops"][0]["err"]')
  [[ $ok == 0 && $err != 0 ]] || {
    echo "negative control broke: plaintext ok=$ok err=$err" >&2
    return 1
  }
}

w9_tier() {
  # Load leg (oracle deferred to post-load, soak.sh pattern).
  run_bench w9 --scenario=soak "--rate=$W9_RATE" "--duration-secs=$W9_SECS" \
    --ack-log=/out/ack-w9.jsonl --no-oracle &
  local load_pid=$!
  sleep "$W9_TRANSFER_AT_SECS"
  # Find the leader via the host Prometheus, transfer to another voter
  # (rr-admin over mTLS + admin token, running in-network via probe).
  local leader to
  leader=$(curl -fsS "$PROM_HOST/api/v1/query?query=rr_raft_is_leader%3D%3D1" 2>/dev/null \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["metric"].get("instance",""))' \
    | cut -d: -f1 || true)
  case "$leader" in
    kv-1) to=2 ;; kv-2) to=3 ;; *) to=1 ;;
  esac
  if [[ -n $leader ]]; then
    log "[w9] leader=$leader → transfer-leader to node $to"
    # TransferLeader must be invoked on the TARGET node's admin API (the
    # server rejects transfer requests addressed at the current leader).
    # `-e RR_ADMIN_TOKEN` (bare name) makes compose resolve the value from
    # this process's exported environment — the token stays OFF the compose
    # argv, where `ps` would expose it (`.env` is sourced with `set -a`, so
    # the variable is guaranteed exported).
    "${COMPOSE[@]}" --profile selfcheck run --rm \
      -e RR_ADMIN_TOKEN \
      probe -c "rr-admin --addr kv-$to:52001 \
        --tls-ca /etc/rr/certs/ca.pem --tls-cert /etc/rr/certs/client.pem --tls-key /etc/rr/certs/client.key \
        --token-env RR_ADMIN_TOKEN \
        group transfer-leader --group 1 --to $to" \
      >>"$OUT/bench-w9.log" 2>&1 || log "[w9] transfer-leader failed (non-fatal)"
  else
    log "[w9] leader not found via Prom — skipping transfer (non-fatal)"
  fi
  wait "$load_pid"
  local load_rc=$?
  # Always run the oracle: it is the authoritative consistency verdict on the
  # ack log (replay + survival reads of the actually-acked writes). The load
  # leg's own exit code additionally reflects bench-side probe health (the
  # periodic single-key read-back), which has produced false alarms the oracle
  # disproves — so the tier verdict is: oracle failure => fail; load-leg
  # failure with a clean oracle => fail too, but the report must distinguish
  # "cluster broke" from "bench probe issue".
  local oracle_rc=0
  run_bench w9-oracle --scenario=soak --oracle-only --ack-log=/out/ack-w9.jsonl || oracle_rc=$?
  if [[ $oracle_rc != 0 ]]; then
    log "[w9] oracle FAILED (rc=$oracle_rc) — real consistency problem"
    for n in 1 2 3; do docker logs "perf-kv-$n" >"$OUT/node$n-w9.log" 2>&1 || true; done
    return 1
  fi
  if [[ $load_rc != 0 ]]; then
    log "[w9] load leg failed (rc=$load_rc) but oracle clean — likely bench probe issue; dumping node logs"
    for n in 1 2 3; do docker logs "perf-kv-$n" >"$OUT/node$n-w9.log" 2>&1 || true; done
    return 1
  fi
  return 0
}

w7_tier() {
  log "[w7] recreating kv nodes with snapshot threshold $W7_SNAPSHOT"
  SNAPSHOT_LOGS_SINCE_LAST=$W7_SNAPSHOT "${COMPOSE[@]}" up -d >/dev/null
  local n bench_rc=0
  for n in 1 2 3; do wait_healthy "perf-kv-$n" 60 || return 1; done
  # NOTE: tiers run under `tier ... || rc=$?`, which suspends set -e inside
  # the body — capture and propagate a bench failure explicitly or the tier
  # would be recorded ok after a failed run.
  run_bench w7 --scenario=wladder "--ladder-rates=$W7_RATES" \
    "--duration-secs=$W1_RUNG_SECS" --warmup-secs=10 \
    --timeseries-out=/out/ts-w7.jsonl || bench_rc=$?
  # Restore the default threshold for later phases (even on bench failure).
  SNAPSHOT_LOGS_SINCE_LAST=50000 "${COMPOSE[@]}" up -d >/dev/null
  for n in 1 2 3; do wait_healthy "perf-kv-$n" 60 || return 1; done
  return "$bench_rc"
}

w6_tier() {
  stack_up "kafka overlay" -f "$KAFKA_YML"
  local start end win prod_rc
  start=$(date +%s)
  # `compose run producer <args>` REPLACES the service's command list — so
  # the full argv is restated here (brokers/topic included).
  "${COMPOSE[@]}" --profile kafka run --rm \
    producer --brokers=redpanda:9092 --topic=perf_1 \
    "--rate=$W6_PRODUCER_RATE" "--total=$((W6_PRODUCER_RATE * W6_SECS))" \
    >"$OUT/producer-w6.log" 2>&1 || {
      prod_rc=$?
      log "[w6] producer exited rc=$prod_rc — see $OUT/producer-w6.log"
      return 1
    }
  end=$(date +%s)
  win=$((end - start))
  # Server-side ingest summary from the host Prometheus port (bench is not
  # the load source in W6, so the script does the window query directly).
  # On query failure write an error placeholder — an empty raw-w6.json would
  # crash the report renderer's json.load at the end of a 2h matrix.
  if ! python3 - "$PROM_HOST" "$end" "$win" >"$OUT/raw-w6.json" <<'PYEOF'
import json, sys, urllib.request, urllib.parse

prom, end, win = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])

def q(expr):
    url = f"{prom}/api/v1/query?" + urllib.parse.urlencode({"query": expr, "time": end})
    with urllib.request.urlopen(url, timeout=15) as r:
        res = json.load(r)["data"]["result"]
    if not res:
        return None
    v = float(res[0]["value"][1])
    return None if v != v else v  # NaN → null

out = {
    "window_secs": win,
    "ingestor_batch_commit_rate": q(f'sum(rate(rr_ingestor_commands_committed_total[{win}s]))'),
    "ingestor_batch_p99_us": (lambda v: v * 1e6 if v is not None else None)(
        q(f'histogram_quantile(0.99, sum by (le) (increase(rr_proposal_duration_seconds_bucket{{source="batch"}}[{win}s])))')),
    "consumer_lag_max": q(f'max(max_over_time(rr_raft_replication_lag[{win}s]))'),
}
print(json.dumps(out, indent=2))
PYEOF
  then
    log "[w6] prometheus window query failed — writing error placeholder"
    printf '{"window_secs": %s, "error": "prometheus query failed"}\n' "$win" >"$OUT/raw-w6.json"
  fi
  tail -3 "$OUT/producer-w6.log"
}

w11_tier() {
  stack_up "mem-log overlay (W11 control)" -f "$MEMLOG_YML"
  run_bench w11 --scenario=wladder "--ladder-rates=$W11_RATES" \
    "--duration-secs=$W1_RUNG_SECS" --warmup-secs=10 \
    --timeseries-out=/out/ts-w11.jsonl
}

w8_tier() {
  stack_up "insecure overlay (W8 control)" -f "$INSECURE_YML"
  run_bench_plain w8 --scenario=wladder "--ladder-rates=$W8_RATES" \
    "--duration-secs=$W1_RUNG_SECS" --warmup-secs=10 \
    --timeseries-out=/out/ts-w8.jsonl
}

# ── preflight ────────────────────────────────────────────────────────────────
command -v docker >/dev/null || fail "docker not found"
docker compose version >/dev/null 2>&1 || fail "docker compose v2 not found"
python3 -V >/dev/null || fail "python3 not found"
command -v curl >/dev/null || fail "curl not found"
[[ -f $PERF_DIR/.env ]] || fail "$PERF_DIR/.env missing — cp $PERF_DIR/.env.example $PERF_DIR/.env and edit tokens"
[[ -f $PERF_DIR/certs/ca.pem ]] || { log "generating certs…"; $PERF_DIR/gen-certs.sh; }
# shellcheck source=/dev/null
set -a; source "$PERF_DIR/.env"; set +a
: "${RR_ADMIN_TOKEN:?RR_ADMIN_TOKEN missing in $PERF_DIR/.env}"
: "${RR_CLIENT_TOKEN:?RR_CLIENT_TOKEN missing in $PERF_DIR/.env}"

# Build the bench image ONCE (local base images only; per-phase builds would
# re-check manifests over the network). The redpanda image (W6) must already
# be local — it is shared with deploy/lab.
log "building rr-perf:dev (once; incremental via BuildKit cache)…"
docker compose -f "$BASE_YML" build >/dev/null || fail "image build failed"
docker image inspect docker.redpanda.com/redpandadata/redpanda:latest >/dev/null 2>&1 \
  || fail "redpanda image missing locally (W6 needs the lab-shared image; load it first)"

log "archive dir: $OUT (git $GIT_SHA, smoke=$SMOKE)"
{
  echo "date=$TS"
  echo "git_sha=$GIT_SHA"
  echo "smoke=$SMOKE"
  echo "uname=$(uname -srm)"
  echo "docker=$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo unknown)"
  echo "workloads=$WORKLOADS"
} >"$OUT/meta.txt"

# ══ Phase A: base secure stack (sec gate + W1–W5 + W9) ═══════════════════════
stack_up "secure (base)"

tier sec "plaintext negative control" sec_tier
# The security gate is not a degradable tier: if a plaintext client got
# served, every later number is measured on a broken stack — stop.
grep -q '^sec fail' "$LEDGER" && fail "security negative control failed"

# W1 — write rate ladder per payload size (knee detection).
for payload in $W1_PAYLOADS; do
  tier w1 "write ladder payload=${payload}B" \
    run_bench "w1-$payload" --scenario=wladder "--ladder-rates=$W1_RATES" \
      "--duration-secs=$W1_RUNG_SECS" --warmup-secs=10 "--payload=$payload" \
      "--timeseries-out=/out/ts-w1-$payload.jsonl"
done

# W2 — read/write mixes (local reads squeeze on write commit latency).
for mix in "write:90,local:10" "write:50,local:50"; do
  tag=$(printf '%s' "$mix" | tr ':,' '__')
  tier w2 "mix $mix @ $W2_RATE/s" \
    run_bench "w2-$tag" --scenario=mix "--rate=$W2_RATE" "--mix=$mix" \
      "--duration-secs=$W2_W5_SECS" --warmup-secs=5 \
      "--timeseries-out=/out/ts-w2-$tag.jsonl"
done

# W3 — linearizable-read share ladder (rest local reads).
for mix in "local:100" "local:90,lin:10" "local:50,lin:50"; do
  tag=$(printf '%s' "$mix" | tr ':,' '__')
  tier w3 "read mix $mix @ $W3_RATE/s" \
    run_bench "w3-$tag" --scenario=mix "--rate=$W3_RATE" "--mix=$mix" \
      "--duration-secs=$W2_W5_SECS" --warmup-secs=5
done

# W4 — hot keys: zipfian vs uniform, same write rate.
for dist in zipfian uniform; do
  tier w4 "write $dist @ $W4_RATE/s" \
    run_bench "w4-$dist" --scenario=mix "--rate=$W4_RATE" --mix=write:100 \
      "--key-dist=$dist" "--key-space=$W4_KEY_SPACE" \
      "--duration-secs=$W2_W5_SECS" --warmup-secs=5 \
      "--timeseries-out=/out/ts-w4-$dist.jsonl"
done

# W5 — payload scan at fixed rate.
for payload in $W5_PAYLOADS; do
  tier w5 "write payload=${payload}B @ $W5_RATE/s" \
    run_bench "w5-$payload" --scenario=mix "--rate=$W5_RATE" --mix=write:100 \
      "--payload=$payload" "--duration-secs=$W2_W5_SECS" --warmup-secs=5
done

# W9 — production-config soak + mid-run leadership transfer (G5 stability).
tier w9 "soak ${W9_SECS}s @ $W9_RATE/s + TransferLeader" w9_tier

# ══ Phase B: snapshot-pressure (W7) — recreate kv nodes with a low threshold ═
tier w7 "snapshot pressure (${W7_SNAPSHOT}) wladder" w7_tier

# ══ Phase C: Kafka ingest (W6) — overlay restart ═════════════════════════════
tier w6 "kafka ingest @ $W6_PRODUCER_RATE/s for ${W6_SECS}s" w6_tier

# ══ Phase D: mem raft-log control (W11) — overlay restart ════════════════════
tier w11 "mem-log control wladder" w11_tier

# ══ Phase E: insecure control (W8) — overlay restart ═════════════════════════
tier w8 "insecure control wladder" w8_tier

# ──收尾:docker stats + report ────────────────────────────────────────────────
log "capturing docker stats + rendering report…"
docker stats --no-stream --format '{{json .}}' perf-kv-1 perf-kv-2 perf-kv-3 \
  >"$OUT/docker-stats.json" 2>/dev/null || true

python3 scripts/prod-perf-report.py "$OUT" >/dev/null
log "report: $OUT/report.md"

if grep -q ' fail ' "$LEDGER"; then
  log "MATRIX DONE WITH FAILURES — see $LEDGER"
  exit 1
fi
log "MATRIX PASS — $OUT"

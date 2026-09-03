#!/usr/bin/env bash
# Soak test: run the lab stack under a fixed mixed workload for hours,
# sampling cluster health and container resources, then emit a report.
#
# Usage (repo root):
#   ./deploy/lab/soak.sh [duration-secs]   # default 7200 (2h)
#
# Outputs: deploy/lab/reports/soak-<timestamp>.md (+ .samples.csv)
set -euo pipefail
cd "$(dirname "$0")/../.."

DURATION=${1:-7200}
TS=$(date +%Y%m%d-%H%M%S)
REPORT_DIR=deploy/lab/reports
REPORT="$REPORT_DIR/soak-$TS.md"
SAMPLES="$REPORT_DIR/soak-$TS.samples.csv"
mkdir -p "$REPORT_DIR"

COMPOSE="docker compose -f deploy/lab/docker-compose.yml"
CONCURRENCY=${SOAK_CONCURRENCY:-50}
PAYLOAD=${SOAK_PAYLOAD:-128}

log() { printf '[soak %s] %s\n' "$(date +%H:%M:%S)" "$*"; }

log "starting lab stack (duration=${DURATION}s, c=$CONCURRENCY, payload=${PAYLOAD}B)…"
$COMPOSE down -v >/dev/null 2>&1 || true  # clean slate: small logs, fresh state
$COMPOSE up -d --build

log "waiting for cluster leader…"
for i in $(seq 1 60); do
  for n in 1 2 3; do
    if docker exec "kv-$n" rr-admin --addr 127.0.0.1:52001 --output json group status --group 1 2>/dev/null \
        | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin)["is_leader"] else 1)' 2>/dev/null; then
      LEADER=$n; break 2
    fi
  done
  sleep 1
done
: "${LEADER:?no leader after 60s}"
log "leader=kv-$LEADER"

# Continuous client load inside the lab network.
log "starting load generator…"
$COMPOSE --profile bench run -d --name rr-lab-soak-load --rm bench \
  --cluster-addrs=kv-1:51001,kv-2:51001,kv-3:51001 \
  --scenario=s2 --concurrency "$CONCURRENCY" --payload "$PAYLOAD" \
  --warmup-secs=10 --duration-secs="$DURATION" >/dev/null

# Sampler: leader changes, per-node last_log_index, RSS/CPU, plus producer
# counter progression if the producer container is running.
echo "ts,leader,lli1,lli2,lli3,rss1_mb,rss2_mb,rss3_mb,cpu1,cpu2,cpu3,counter" >"$SAMPLES"
term_at_start=$(docker exec "kv-$LEADER" rr-admin --addr 127.0.0.1:52001 --output json group status --group 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["current_term"])')

START=$(date +%s)
while (( $(date +%s) - START < DURATION )); do
  sleep 60
  el=$(( $(date +%s) - START ))
  l=""; idx=(); 
  for n in 1 2 3; do
    st=$(docker exec "kv-$n" rr-admin --addr 127.0.0.1:52001 --output json group status --group 1 2>/dev/null || echo '{}')
    is_l=$(printf '%s' "$st" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("is_leader",False))' 2>/dev/null || echo '?')
    [[ "$is_l" == "True" ]] && l=$n
    idx+=("$(printf '%s' "$st" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("last_log_index",-1))' 2>/dev/null || echo -1)")
  done
  stats=$(docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' kv-1 kv-2 kv-3 2>/dev/null || true)
  rss=(); cpu=()
  for n in 1 2 3; do
    line=$(printf '%s\n' "$stats" | grep "kv-$n " || echo "? 0MiB 0%")
    rss+=("$(printf '%s' "$line" | grep -oE '[0-9.]+MiB' | head -1 | tr -d 'MiB')")
    cpu+=("$(printf '%s' "$line" | grep -oE '[0-9.]+%' | head -1 | tr -d '%')")
  done
  counter=""
  if [[ -n "$(docker ps -q -f name=rr-lab-producer)" ]]; then
    counter=$(printf 'get lab:counter\n' | ./target/release/rr-admin repl --addr "127.0.0.1:5110${l:-1}" --group 1 2>/dev/null | grep -oE '[0-9]+' | tail -1 || true)
  fi
  echo "$(date +%s),${l:-},${idx[0]},${idx[1]},${idx[2]},${rss[0]:-},${rss[1]:-},${rss[2]:-},${cpu[0]:-},${cpu[1]:-},${cpu[2]:-},$counter" >>"$SAMPLES"
  log "t+${el}s leader=kv-${l:-?} lli=${idx[0]}/${idx[1]}/${idx[2]} rss_mb=${rss[0]:-?}/${rss[1]:-?}/${rss[2]:-?} counter=${counter:-n/a}"
done

log "load window over; collecting bench summary…"
docker logs rr-lab-soak-load >"$REPORT_DIR/soak-$TS.bench.log" 2>&1 || true
bench_err_rows=$(grep -E '^  [0-9]+ +[0-9]' "$REPORT_DIR/soak-$TS.bench.log" | awk '{if ($NF != "0") bad++} END {print bad+0}')
bench_err_total=$(grep -E '^  [0-9]+ +[0-9]' "$REPORT_DIR/soak-$TS.bench.log" | awk '{s+=$NF} END {print s+0}')
term_at_end=$(docker exec kv-1 rr-admin --addr 127.0.0.1:52001 --output json group status --group 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["current_term"])')
elections=$((term_at_end - term_at_start))

# Report
{
  echo "# Soak report — $TS"
  echo
  echo "- duration: ${DURATION}s | workload: S2 writes c=$CONCURRENCY payload=${PAYLOAD}B (in-network)"
  echo "- bench error rows: $bench_err_rows (total errored requests: $bench_err_total)"
  echo "- elections during soak (term delta): $elections"
  echo "- samples: $(tail -n +2 "$SAMPLES" | wc -l | tr -d ' ') minute-rows in $(basename "$SAMPLES")"
  echo
  echo '## RSS (MiB) first/last per node'
  awk -F, 'NR==2{f6=$6;f7=$7;f8=$8} END{printf "kv-1: %s -> %s\nkv-2: %s -> %s\nkv-3: %s -> %s\n", f6,$6,f7,$7,f8,$8}' "$SAMPLES"
  echo
  echo '## p99 drift (S2 level window)'
  grep -E '^  [0-9]+ ' "$REPORT_DIR/soak-$TS.bench.log" | head -3 || true
  echo
  echo "raw bench log: soak-$TS.bench.log"
} >"$REPORT"

log "report written: $REPORT"
# Pass criteria
[[ "$bench_err_total" == "0" ]] || { log "SOAK FAIL: $bench_err_total errored requests"; exit 1; }
[[ "$elections" -le 1 ]] || { log "SOAK FAIL: $elections unexpected elections"; exit 1; }
log "SOAK PASS"

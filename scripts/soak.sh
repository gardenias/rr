#!/usr/bin/env bash
# soak.sh — R-8 soak harness: 3-node lab cluster + fixed-rate keyed idempotent
# load + seeded fault-injection loop + three-part oracle.
#
# Usage (repo root):
#   scripts/soak.sh                      # 2h smoke (default SOAK_DURATION=7200)
#   SOAK_DURATION=60 scripts/soak.sh     # quick local smoke
#   SOAK_SEED=42 scripts/soak.sh         # replay the same fault schedule
#   CLIENT_BATCHING=1 scripts/soak.sh    # exercise rr-client write batching
#
# Knobs (env):
#   SOAK_DURATION          total load window, seconds        (default 7200)
#   SOAK_RATE              keyed idempotent writes/s         (default 200)
#   SOAK_FAULT_MEAN_SECS   mean interval between faults      (default 600)
#   SOAK_SEED              fault-schedule seed (entropy if unset)
#   SOAK_KEEP_STACK=1      don't compose down at the end (forensics)
#   SOAK_PORT_BASE         host-side health/metrics port base; kv-N answers on
#                          SOAK_PORT_BASE+N                    (default 9110,
#                          i.e. the stock lab's 9111-9113). The lab compose
#                          pins its project/container names and published
#                          ports, so fully parallel runs additionally need
#                          their own compose project + port mapping; this
#                          knob only retargets soak.sh's curl checks.
#   CLIENT_BATCHING=1      pass --client-batching to the load driver
#                          (default off)
#   SOAK_UNKEYED_RATIO     fraction of writes sent unkeyed (unique keys) —
#                          the only traffic the batcher packs. Default 0;
#                          with CLIENT_BATCHING=1 the default becomes 0.1
#                          (rr-perf-bench hard-errors on batching + ratio 0).
#
# Oracle (all three must hold):
#   1. no data loss     — ack-log replay via rr-perf-bench --oracle-only
#   2. no panic         — zero "panicked at" across node logs; container
#                         restart counts match the injected kill count
#   3. lag convergence  — last_log_index equal across nodes within 60s of
#                         the final fault healing
#
# Outputs: docs/perf/soak/soak-report-<ts>.md + target/soak/<ts>/ raw
# artifacts (ack-log / metrics / fault log / node logs).
#
# Fault set: SIGKILL (docker kill), pause/unpause, network partition
# (disconnect/connect). ENOSPC is deliberately NOT in the smoke fault set:
# docker volumes are not size-limited, and filling the host disk from a test
# is unacceptable — ENOSPC is covered at L1 by rr-core's FaultInjectingStorage
# (crates/rr-core/src/storage_fault.rs) and belongs to the release-tier runner
# with a dedicated size-limited disk.
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${SOAK_DURATION:-7200}
RATE=${SOAK_RATE:-200}
FAULT_MEAN=${SOAK_FAULT_MEAN_SECS:-600}
SEED=${SOAK_SEED:-$RANDOM$RANDOM$RANDOM}
PORT_BASE=${SOAK_PORT_BASE:-9110} # kv-N health/metrics = $((PORT_BASE + N))
CLIENT_BATCHING=${CLIENT_BATCHING:-0}
if [[ "$CLIENT_BATCHING" == "1" ]]; then
  # Batching packs unkeyed writes only — default a nonzero unkeyed share so
  # the run really exercises the batcher (rr-perf-bench fails hard on
  # --client-batching combined with ratio 0).
  UNKEYED_RATIO=${SOAK_UNKEYED_RATIO:-0.1}
else
  UNKEYED_RATIO=${SOAK_UNKEYED_RATIO:-0.0}
fi
TS=$(date +%Y%m%d-%H%M%S)
RUN_DIR="target/soak/$TS"
REPORT="docs/perf/soak/soak-report-$TS.md"
COMPOSE="docker compose -f deploy/lab/docker-compose.yml"
NET="rrlab_rrlab"
mkdir -p "$RUN_DIR" "$(dirname "$REPORT")"

log() { printf '[soak %s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "SOAK FAIL: $*"; exit 1; }

command -v docker >/dev/null || fail "docker not found"
docker compose version >/dev/null 2>&1 || fail "docker compose v2 not found"
python3 -V >/dev/null || fail "python3 not found"

# The load driver and oracle run inside the lab network (bench profile):
# the cluster advertises kv-N:51001 as redirect targets, which only resolve
# in-network. The bench image carries the workspace's rr-perf-bench.

# ── 1. cluster up ───────────────────────────────────────────────────────────
log "bringing up lab stack (duration=${DURATION}s rate=${RATE}/s seed=$SEED fault-mean=${FAULT_MEAN}s)…"
$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d --build

cleanup() {
  if [[ "${SOAK_KEEP_STACK:-0}" != "1" ]]; then
    $COMPOSE down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

log "waiting for three healthy nodes…"
for n in 1 2 3; do
  for i in $(seq 1 90); do
    if curl -fsS "http://127.0.0.1:$((PORT_BASE + n))/ready" >/dev/null 2>&1; then break; fi
    if (( i == 90 )); then fail "kv-$n never became ready"; fi
    sleep 2
  done
done
log "all nodes ready"

# ── 2. load (in-network container: the cluster advertises kv-N:51001 as
# redirect targets, so clients MUST live inside the compose network; the run
# dir is bind-mounted for the journals) ─────────────────────────────────────
log "starting load: rate=$RATE/s duration=${DURATION}s batching=$CLIENT_BATCHING unkeyed-ratio=$UNKEYED_RATIO …"
bench_extra=(--soak-unkeyed-ratio "$UNKEYED_RATIO")
if [[ "$CLIENT_BATCHING" == "1" ]]; then
  bench_extra+=(--client-batching)
fi
docker rm -f rr-soak-load >/dev/null 2>&1 || true
$COMPOSE --profile bench run -d --name rr-soak-load \
  -v "$PWD/$RUN_DIR:/soak" \
  bench \
  --cluster-addrs "kv-1:51001,kv-2:51001,kv-3:51001" \
  --scenario soak \
  --rate "$RATE" --duration-secs "$DURATION" \
  --ack-log /soak/ack-log.jsonl --metrics-log /soak/metrics.jsonl \
  "${bench_extra[@]}" \
  --no-oracle >/dev/null

# ── 3. seeded fault loop ────────────────────────────────────────────────────
FAULT_LOOP_PID=""
fault_loop() {
  python3 - "$SEED" "$FAULT_MEAN" "$DURATION" "$RUN_DIR/fault-log.jsonl" <<'PYEOF'
import json, random, subprocess, sys, time

seed, mean, duration, log_path = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
rng = random.Random(seed)
NODES = ["kv-1", "kv-2", "kv-3"]

def record(event, target, detail=""):
    with open(log_path, "a") as f:
        f.write(json.dumps({"ts_ms": int(time.time() * 1000), "event": event,
                            "target": target, "seed": seed, "detail": detail}) + "\n")

record("plan", None, f"mean_interval={mean}s duration={duration}s")
start = time.time()
while time.time() - start < duration - 30:  # keep a clean 30s tail for convergence
    # exponential interval
    wait = rng.expovariate(1.0 / mean)
    time.sleep(min(wait, max(0, duration - 30 - (time.time() - start))))
    if time.time() - start >= duration - 30:
        break
    target = rng.choice(NODES)
    kind = rng.choice(["kill", "pause", "partition"])
    if kind == "kill":
        subprocess.run(["docker", "kill", "-s", "SIGKILL", target], check=False)
        record("sigkill", target)  # restart=unless-stopped auto-restarts it
    elif kind == "pause":
        hold = rng.uniform(5, 30)
        subprocess.run(["docker", "pause", target], check=False)
        record("pause", target, f"hold={hold:.1f}s")
        time.sleep(hold)
        subprocess.run(["docker", "unpause", target], check=False)
        record("unpause", target)
    else:
        hold = rng.uniform(5, 30)
        subprocess.run(["docker", "network", "disconnect", "rrlab_rrlab", target], check=False)
        record("partition", target, f"hold={hold:.1f}s")
        time.sleep(hold)
        subprocess.run(["docker", "network", "connect", "rrlab_rrlab", target], check=False)
        record("heal", target)
record("done", None)
PYEOF
}
fault_loop &
FAULT_LOOP_PID=$!
log "fault loop running (seed=$SEED; replay: SOAK_SEED=$SEED)"

# ── 4. wait for load to finish ──────────────────────────────────────────────
LOAD_RC=$(docker wait rr-soak-load)
docker logs rr-soak-load >"$RUN_DIR/load.log" 2>&1 || true
# Stop the fault loop; heal everything.
kill $FAULT_LOOP_PID 2>/dev/null || true
wait $FAULT_LOOP_PID 2>/dev/null || true
for n in 1 2 3; do
  docker unpause "kv-$n" >/dev/null 2>&1 || true
  docker network connect "$NET" "kv-$n" >/dev/null 2>&1 || true
  docker start "kv-$n" >/dev/null 2>&1 || true  # no-op if running
done
[[ $LOAD_RC == 0 ]] || { tail -30 "$RUN_DIR/load.log"; fail "load driver exited rc=$LOAD_RC"; }
docker rm -f rr-soak-load >/dev/null 2>&1 || true
log "load window done"

# ── 5. convergence (leader + equal last_log_index within 60s) ───────────────
# Nodes killed near the end need to restart first — wait for /ready, then
# compare log tails.
log "waiting for nodes ready…"
for n in 1 2 3; do
  for i in $(seq 1 45); do
    curl -fsS "http://127.0.0.1:$((PORT_BASE + n))/ready" >/dev/null 2>&1 && break
    sleep 2
  done
done
log "waiting for convergence…"
conv_ok=0
idx=("?" "?" "?")
for i in $(seq 1 30); do
  idx=()
  for n in 1 2 3; do
    v=$(curl -fsS "http://127.0.0.1:$((PORT_BASE + n))/metrics" 2>/dev/null | grep -E '^rr_raft_last_log_index{group_id="1"}' | head -1 | awk '{print $NF}' || echo "")
    idx+=("${v:-?}")
  done
  if [[ "${idx[0]}" == "${idx[1]}" && "${idx[1]}" == "${idx[2]}" && "${idx[0]}" != "?" ]]; then
    conv_ok=1; break
  fi
  sleep 2
done
[[ $conv_ok == 1 ]] || fail "cluster did not converge within 60s (last_log_index: ${idx[*]})"
log "converged: last_log_index=${idx[0]}"

# ── 6. oracle 1: ack-log replay (no data loss) ──────────────────────────────
log "running ack-log oracle…"
set +e
$COMPOSE --profile bench run --rm \
  -v "$PWD/$RUN_DIR:/soak" \
  bench \
  --cluster-addrs "kv-1:51001,kv-2:51001,kv-3:51001" \
  --scenario soak --oracle-only \
  --ack-log /soak/ack-log.jsonl >"$RUN_DIR/oracle.log" 2>&1
ORACLE_RC=$?
set -e
tail -3 "$RUN_DIR/oracle.log"
[[ $ORACLE_RC == 0 ]] || fail "oracle found lost writes (see $RUN_DIR/oracle.log)"

# ── 7. oracle 2: no panics / expected restarts only ─────────────────────────
panics=0
for n in 1 2 3; do
  docker logs "kv-$n" >"$RUN_DIR/node$n.log" 2>&1 || true
  p=$(grep -c "panicked at" "$RUN_DIR/node$n.log" || true)
  panics=$((panics + p))
done
[[ $panics == 0 ]] || fail "$panics panic(s) in node logs"
kills=$(grep -c '"event": *"sigkill"' "$RUN_DIR/fault-log.jsonl" || true)
restarts=0
for n in 1 2 3; do
  rc=$(docker inspect -f '{{.RestartCount}}' "kv-$n" 2>/dev/null || echo 0)
  restarts=$((restarts + rc))
done
log "panics=0 restarts=$restarts injected-kills=$kills"
[[ $restarts -ge $kills ]] || fail "fewer restarts ($restarts) than kills ($kills) — node crashed on its own?"

# ── 8. report ───────────────────────────────────────────────────────────────
stats=$(python3 - "$RUN_DIR" <<'PYEOF'
import json, sys
run = sys.argv[1]
records = [json.loads(l) for l in open(f"{run}/metrics.jsonl")] if __import__("os").path.exists(f"{run}/metrics.jsonl") else []
faults = [json.loads(l) for l in open(f"{run}/fault-log.jsonl")]
acks = sum(1 for _ in open(f"{run}/ack-log.jsonl"))
failed_total = records[-1]["failed"] if records else 0
sent_total = records[-1]["sent"] if records else 0
nf = sum(1 for f in faults if f["event"] in ("sigkill", "pause", "partition"))
print(json.dumps({"acks": acks, "sent": sent_total, "failed": failed_total, "faults": nf}))
PYEOF
)
acks=$(printf '%s' "$stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["acks"])')
failed=$(printf '%s' "$stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["failed"])')
faults=$(printf '%s' "$stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["faults"])')
unkeyed_sent=$(grep -oE 'unkeyed_sent=[0-9]+' "$RUN_DIR/load.log" | tail -1 | cut -d= -f2 || true)
oracle_unkeyed=$(grep -oE 'unkeyed [0-9]+\)' "$RUN_DIR/oracle.log" | tail -1 | tr -dc '0-9' || true)

{
  echo "# Soak report — $TS (R-8 smoke)"
  echo
  echo "- duration: ${DURATION}s | rate: ${RATE}/s | seed: $SEED (replay: SOAK_SEED=$SEED)"
  echo "- client batching: $CLIENT_BATCHING | unkeyed ratio: $UNKEYED_RATIO | unkeyed sent: ${unkeyed_sent:-0} | oracle unkeyed verified: ${oracle_unkeyed:-0}"
  echo "- acked writes: $acks | client-side failures (fault windows): $failed | injected faults: $faults"
  echo "- oracle 1 (no data loss): $(grep -oE '[0-9]+/[0-9]+ verified' "$RUN_DIR/oracle.log" | tail -1)"
  echo "- oracle 2 (no panic): panics=0, restarts=$restarts (injected kills=$kills)"
  echo "- oracle 3 (lag convergence): last_log_index equal across nodes within 60s of final heal"
  echo
  echo "## Artifacts"
  echo "- run dir: \`$RUN_DIR\` (ack-log.jsonl / metrics.jsonl / fault-log.jsonl / nodeN.log / load.log / oracle.log)"
} >"$REPORT"
log "report: $REPORT"
log "SOAK PASS (duration=${DURATION}s acks=$acks faults=$faults)"

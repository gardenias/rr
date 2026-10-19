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
#   SOAK_WITH_OBSERVABILITY=1  also start the lab's prometheus + grafana
#                          (purely visual; the soak oracle never queries them,
#                          and their images require Docker Hub access)
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
#   SOAK_ZERO_TPS_MAX_WINDOWS
#                          availability SLO: fail when more than this many
#                          consecutive metrics-journal windows (300s each by
#                          bench default) show window_tps == 0 while load is
#                          still far from done         (default 10 ≈ 50min)
#
# Oracle (all must hold):
#   0. availability SLO — no >SOAK_ZERO_TPS_MAX_WINDOWS-run of zero-tps
#                         metrics windows ending well before load end (§4b)
#   1. no data loss     — ack-log replay via rr-perf-bench --oracle-only
#   2. no panic         — zero "panicked at" across node logs; container
#                         restart counts match the injected kill count
#   3. lag convergence  — last_log_index equal across nodes within 60s of
#                         the final fault healing, AND (keyed-only
#                         unbatched runs; warn-only otherwise) converged
#                         last_log_index >= final acked count (§5b)
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
# Availability SLO gate (§4b): longest tolerable run of consecutive
# zero-throughput metrics windows mid-run. 10 × the bench's default 300s
# metrics interval ≈ 50 minutes of silence — far beyond any single injected
# fault's 5-30s hold, so a trip means a wedge the cluster never recovered
# from on its own (run 38045591646 sat at zero tps for 101 minutes).
ZERO_TPS_MAX_WINDOWS=${SOAK_ZERO_TPS_MAX_WINDOWS:-10}
TS=$(date +%Y%m%d-%H%M%S)
RUN_DIR="target/soak/$TS"
REPORT="docs/perf/soak/soak-report-$TS.md"
COMPOSE="docker compose -f deploy/lab/docker-compose.yml"
NET="rrlab_rrlab"
mkdir -p "$RUN_DIR" "$(dirname "$REPORT")"
# The bench/oracle containers run as non-root (deploy/lab/Dockerfile USER
# rr:rr) and write ack-log/metrics journals into the bind-mounted /soak —
# the default 0755 root-owned-ish mode fails with EACCES on the runner
# ("create journal /soak/metrics.jsonl: Permission denied", soak-nightly
# run 38042126854). This is CI-local scratch space, so open it up.
chmod 0777 "$RUN_DIR"

log() { printf '[soak %s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "SOAK FAIL: $*"; exit 1; }

command -v docker >/dev/null || fail "docker not found"
docker compose version >/dev/null 2>&1 || fail "docker compose v2 not found"
python3 -V >/dev/null || fail "python3 not found"

# The load driver and oracle run inside the lab network (bench profile):
# the cluster advertises kv-N:51001 as redirect targets, which only resolve
# in-network. The bench image carries the workspace's rr-perf-bench.

# ── 1. cluster up ───────────────────────────────────────────────────────────
# The soak oracle never queries Prometheus/Grafana, so only the kv services
# are started: their rr-lab:dev image is built locally, while prom/grafana
# must come from Docker Hub (offline/limited environments cannot pull them).
# SOAK_WITH_OBSERVABILITY=1 also starts prometheus + grafana for eyeballing.
log "bringing up lab stack (duration=${DURATION}s rate=${RATE}/s seed=$SEED fault-mean=${FAULT_MEAN}s)…"
SERVICES=(kv-1 kv-2 kv-3)
if [[ "${SOAK_WITH_OBSERVABILITY:-0}" == "1" ]]; then
  SERVICES+=(prometheus grafana)
fi
$COMPOSE down -v >/dev/null 2>&1 || true
if ! $COMPOSE up -d --build "${SERVICES[@]}"; then
  # Forensics BEFORE the teardown trap wipes the stack: a node that dies
  # during startup (bad flag, missing env, crash) otherwise leaves no logs
  # behind and the failure is undebuggable from artifacts alone.
  $COMPOSE logs --no-color kv-1 kv-2 kv-3 >"$RUN_DIR/compose-up.log" 2>&1 || true
  tail -40 "$RUN_DIR/compose-up.log" || true
  fail "lab stack failed to start (see $RUN_DIR/compose-up.log)"
fi

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
    if (( i == 90 )); then
      # Same forensics as the up-failure path: the teardown trap wipes the
      # stack, and a node that is compose-Healthy but unreachable on its
      # published port (e.g. metrics bound to loopback in the container)
      # otherwise leaves no evidence behind.
      $COMPOSE logs --no-color kv-1 kv-2 kv-3 >"$RUN_DIR/compose-up.log" 2>&1 || true
      tail -40 "$RUN_DIR/compose-up.log" || true
      fail "kv-$n never became ready (see $RUN_DIR/compose-up.log)"
    fi
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

# ── 4b. oracle 0: availability SLO — no silent zero-throughput streak ───────
# The loss oracle (§6) verifies that ACKED writes survive; it says nothing
# about a cluster that simply stops acking while the driver keeps sending.
# Run 38045591646 sat at window_tps=0 for 101 consecutive minutes (rounds
# 10-11, no fault injected) and nearly passed — only the restart-count gate
# caught it. Fail on any run of more than ZERO_TPS_MAX_WINDOWS consecutive
# zero-tps windows that ENDS well before the load stopped. A streak abutting
# the final record is out of scope here: the drain tail can legitimately dip
# (the post-loop grace aborts in-flight ops, which then never ack), and a
# wedge running into the tail is covered by the convergence (§5) and
# magnitude (§5b) gates instead.
zero_streak_len="?"
if [[ -s "$RUN_DIR/metrics.jsonl" ]]; then
  zero_slo=$(python3 - "$RUN_DIR/metrics.jsonl" "$ZERO_TPS_MAX_WINDOWS" <<'PYEOF'
import json, sys

path, max_windows = sys.argv[1], int(sys.argv[2])
records = []
with open(path) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pass  # a torn final line (kill mid-append) must not crash the gate

# Fewer than 2 records (e.g. a smoke shorter than the 300s interval) cannot
# exhibit a streak — nothing to judge.
if len(records) < 2:
    print("")
    sys.exit(0)

# Window length from the journal itself (soak.sh does not pass
# --metrics-snapshot-secs), so the verdict survives an interval override.
deltas = sorted(b["ts_ms"] - a["ts_ms"] for a, b in zip(records, records[1:]))
interval_ms = max(deltas[len(deltas) // 2], 1)
end_ts = records[-1]["ts_ms"]

# Collect every maximal run of consecutive zero-tps windows. A missing or
# unparseable window_tps counts as NON-zero: on a corrupt journal the gate
# may err towards silence, never towards a false FAIL.
streaks = []
run = []
for r in records:
    try:
        zero = float(r["window_tps"]) == 0.0
    except (KeyError, TypeError, ValueError):
        zero = False
    if zero:
        run.append(r)
    else:
        if run:
            streaks.append(run)
        run = []
if run:
    streaks.append(run)

# "Far from load end": the streak's last zero window precedes the final
# record by more than one full interval.
def mid_run(streak):
    return end_ts - streak[-1]["ts_ms"] > interval_ms

violating = [s for s in streaks if len(s) > max_windows and mid_run(s)]
worst = max(streaks, key=len, default=[])
shown = max(violating, key=len) if violating else worst
print(f"{len(shown)} {1 if violating else 0} {len(shown) * interval_ms // 60000}")
PYEOF
)
  if [[ -n "$zero_slo" ]]; then
    read -r zero_streak_len zero_slo_fail zero_streak_min <<<"$zero_slo"
    if [[ "$zero_slo_fail" == "1" ]]; then
      # Forensics BEFORE the teardown trap wipes the stack (same discipline
      # as the §6 node-log capture): a silent wedge's only evidence is in the
      # node logs.
      for n in 1 2 3; do
        docker logs "kv-$n" >"$RUN_DIR/node$n.log" 2>&1 || true
      done
      fail "availability SLO violated: ${zero_streak_len} consecutive zero-tps windows (~${zero_streak_min}min, limit ${ZERO_TPS_MAX_WINDOWS}) ending well before load end — silent wedge (run 38045591646 rounds 10-11 shape)"
    fi
    log "availability SLO ok: longest zero-tps streak ${zero_streak_len} window(s) (limit ${ZERO_TPS_MAX_WINDOWS}, mid-run only)"
  else
    log "availability SLO skipped: metrics.jsonl has <2 records (run shorter than the metrics interval)"
  fi
else
  log "WARN: availability SLO gate skipped — $RUN_DIR/metrics.jsonl missing/empty"
fi

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

# ── 5b. convergence magnitude check ─────────────────────────────────────────
# Equal last_log_index across nodes is necessary but NOT sufficient: run
# 38045591646 converged on 392176 after ~1.04M committed entries had been
# physically erased — the three nodes agreed on a TRUNCATED log and the
# equality check alone passed. In the keyed-only unbatched shape
# (CLIENT_BATCHING=0, SOAK_UNKEYED_RATIO=0) every acked write commits exactly
# one raft entry and raft indices are absolute (front-compaction by
# snapshotting never lowers last_log_index; blank per-term leader entries and
# membership entries only inflate it), so last_log_index >= acked must hold.
# Batching packs many writes into one entry and breaks the 1-write=1-entry
# premise, so other shapes degrade to warn-only.
# Threshold source: the load journal's final acked count — metrics.jsonl's
# last record, falling back to load.log's "soak load done" summary line (the
# metrics journal has no records at all on runs shorter than the 300s
# interval, e.g. a 60s smoke).
final_acked=$(python3 - "$RUN_DIR" <<'PYEOF'
import json, os, re, sys

run = sys.argv[1]
acked = ""
metrics = os.path.join(run, "metrics.jsonl")
if os.path.exists(metrics):
    last = ""
    with open(metrics) as f:
        for line in f:
            if line.strip():
                last = line
    if last:
        try:
            acked = str(int(json.loads(last)["acked"]))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            acked = ""
if not acked:
    load = os.path.join(run, "load.log")
    if os.path.exists(load):
        # "soak load done: sent=... acked=N ..." — rr-perf-bench's final
        # summary line; take the last match.
        found = None
        with open(load, errors="replace") as f:
            for line in f:
                m = re.search(r"\bsoak load done:.*\backed=(\d+)", line)
                if m:
                    found = m
        if found:
            acked = found.group(1)
print(acked)
PYEOF
)
magnitude_mode=skipped
if [[ -z "$final_acked" ]]; then
  log "WARN: convergence magnitude check skipped — no final acked count in metrics.jsonl or load.log"
elif [[ "$CLIENT_BATCHING" == "0" && "$UNKEYED_RATIO" =~ ^0(\.0+)?$ ]]; then
  magnitude_mode=strict
  if (( idx[0] < final_acked )); then
    # Forensics BEFORE the teardown trap wipes the stack — same discipline
    # as the §6 node-log capture.
    for n in 1 2 3; do
      docker logs "kv-$n" >"$RUN_DIR/node$n.log" 2>&1 || true
    done
    fail "converged last_log_index=${idx[0]} < acked=${final_acked} — cluster agreed on a log shorter than the acknowledged writes (committed entries erased; run 38045591646 shape)"
  fi
  log "convergence magnitude ok: last_log_index=${idx[0]} >= acked=${final_acked}"
else
  magnitude_mode=warn
  if (( idx[0] < final_acked )); then
    log "WARN: converged last_log_index=${idx[0]} < acked=${final_acked}, but 1-write=1-entry does not hold with batching=$CLIENT_BATCHING unkeyed-ratio=$UNKEYED_RATIO — advisory only"
  else
    log "convergence magnitude ok (advisory): last_log_index=${idx[0]} >= acked=${final_acked}"
  fi
fi

# ── 6. oracle 1: ack-log replay (no data loss) ──────────────────────────────
# Capture node logs BEFORE the oracle: an INV-1 failure exits via `fail`
# before the §7 capture, and node logs are the only evidence for *why* an
# acked write vanished (soak-nightly run 38045591646 lost them this way).
for n in 1 2 3; do
  docker logs "kv-$n" >"$RUN_DIR/node$n.log" 2>&1 || true
done
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
  echo "- oracle 0 (availability SLO): longest zero-tps streak ${zero_streak_len} window(s), limit ${ZERO_TPS_MAX_WINDOWS} (mid-run only)"
  echo "- convergence magnitude (${magnitude_mode}): last_log_index=${idx[0]} vs final acked=${final_acked:-n/a}"
  echo
  echo "## Artifacts"
  echo "- run dir: \`$RUN_DIR\` (ack-log.jsonl / metrics.jsonl / fault-log.jsonl / nodeN.log / load.log / oracle.log)"
} >"$REPORT"
log "report: $REPORT"
log "SOAK PASS (duration=${DURATION}s acks=$acks faults=$faults)"

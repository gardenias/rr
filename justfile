set shell := ["bash", "-cu"]
set positional-arguments := true

ROOT := justfile_directory()
JUST_STATE := ROOT / "target/just"
RR_CLUSTER_NAME := env_var_or_default("RR_CLUSTER_NAME", "rr-local")
RR_ETCD_IMAGE := env_var_or_default("RR_ETCD_IMAGE", "quay.io/coreos/etcd:v3.5.13")
RR_ETCD_ENDPOINTS := env_var_or_default("RR_ETCD_ENDPOINTS", "http://127.0.0.1:2379")
RR_DOCKER_HOST := env_var_or_default("RR_DOCKER_HOST", env_var_or_default("DOCKER_HOST", ""))
KV_BASE_PORT := env_var_or_default("RR_KV_BASE_PORT", "53101")
GENESIS_BASE_PORT := env_var_or_default("RR_GENESIS_BASE_PORT", "53201")
JAVA_DIR := ROOT / "sdks/java"
CONSOLE_DIR := ROOT / "crates/rr-admin/console"
DOCS_PORT := env_var_or_default("DOCS_PORT", "3000")

default:
    @just --list

[group("core")]
doctor:
    #!/usr/bin/env bash
    set -euo pipefail
    check() {
      local name="$1" required="${2:-false}"
      if command -v "$name" >/dev/null 2>&1; then
        printf 'ok       %-10s %s\n' "$name" "$(command -v "$name")"
      elif [[ "$required" == true ]]; then
        printf 'missing  %-10s required; install it before using this recipe\n' "$name" >&2
        return 1
      else
        printf 'missing  %-10s optional\n' "$name"
      fi
    }
    check just true
    check cargo true
    check rustc true
    check python3 true
    check java false
    check node false
    check npm false
    check helm false
    check docker false
    check colima false
    if [[ -n "{{RR_DOCKER_HOST}}" ]]; then
      printf 'docker   DOCKER_HOST=%s\n' "{{RR_DOCKER_HOST}}"
    fi

[group("core")]
build:
    cargo build

[group("core")]
build-release: console-build
    cargo build --release

[group("console")]
console-install:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v pnpm >/dev/null || { echo "pnpm is required for console-install" >&2; exit 1; }
    pnpm --dir "{{CONSOLE_DIR}}" install --frozen-lockfile

[group("console")]
console-build: console-install
    #!/usr/bin/env bash
    set -euo pipefail
    pnpm --dir "{{CONSOLE_DIR}}" build
    [[ -f "{{CONSOLE_DIR}}/out/index.html" ]] || {
      echo "console export missing: {{CONSOLE_DIR}}/out/index.html" >&2
      exit 1
    }

[group("console")]
console-test: console-build
    pnpm --dir "{{CONSOLE_DIR}}" typecheck
    pnpm --dir "{{CONSOLE_DIR}}" lint
    pnpm --dir "{{CONSOLE_DIR}}" test:unit
    pnpm --dir "{{CONSOLE_DIR}}" test:e2e

[group("core")]
test:
    cargo test

[group("core")]
test-one NAME:
    cargo test '{{NAME}}'

[group("core")]
fmt:
    cargo fmt --all

[group("core")]
check:
    cargo fmt --all -- --check
    ./scripts/check-invariants.sh
    cargo check --workspace --all-targets

[group("core")]
clean-target-just:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -d "{{JUST_STATE}}" ]]; then
      for pid_file in "{{JUST_STATE}}"/*.pid; do
        [[ -e "$pid_file" ]] || continue
        pid="$(cat "$pid_file")"
        if kill -0 "$pid" >/dev/null 2>&1; then
          kill -INT "$pid" >/dev/null 2>&1 || true
        fi
      done
      rm -rf "{{JUST_STATE}}"
    fi

[group("docs")]
docs-fix-page PAGE WRITE="":
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{WRITE}}" in
      "") npm --prefix "{{ROOT}}/docs" run fix-bilingual-page -- "{{PAGE}}" ;;
      "--write") npm --prefix "{{ROOT}}/docs" run fix-bilingual-page -- "{{PAGE}}" --write ;;
      *) echo "usage: just docs-fix-page <page> [--write]" >&2; exit 2 ;;
    esac

[group("docs")]
docs-build:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v npm >/dev/null || { echo "npm is required for docs-build" >&2; exit 1; }
    [[ -d "{{ROOT}}/docs/node_modules" ]] || { echo "docs/node_modules is missing; run npm --prefix docs ci first" >&2; exit 1; }
    npm --prefix "{{ROOT}}/docs" run diagrams
    npm --prefix "{{ROOT}}/docs" run build

[group("docs")]
docs-serve:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v npm >/dev/null || { echo "npm is required for docs-serve" >&2; exit 1; }
    [[ -d "{{ROOT}}/docs/node_modules" ]] || { echo "docs/node_modules is missing; run npm --prefix docs ci first" >&2; exit 1; }
    npm --prefix "{{ROOT}}/docs" run diagrams
    npm --prefix "{{ROOT}}/docs" run start -- --port "{{DOCS_PORT}}"

[group("deploy")]
helm-test:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v helm >/dev/null || { echo "helm is required; install it first" >&2; exit 1; }
    command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
    python3 "{{ROOT}}/deploy/helm/rr/tests/test_formation_config.py"

[group("cluster")]
kv-cluster-up:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{JUST_STATE}}/pids" "{{JUST_STATE}}/logs"
    cargo build -p kv-store -p rr-admin
    roster="1@127.0.0.1:{{KV_BASE_PORT}},2@127.0.0.1:$(( {{KV_BASE_PORT}} + 1 )),3@127.0.0.1:$(( {{KV_BASE_PORT}} + 2 ))"
    start_node() {
      local id="$1" port="$2" extra="$3"
      local pid_file="{{JUST_STATE}}/pids/kv-node${id}.pid"
      [[ ! -e "$pid_file" ]] || { echo "kv node ${id} already has PID state; run just kv-cluster-down first" >&2; exit 1; }
      nohup "{{ROOT}}/target/debug/kv-store" --node-id "$id" --port "$port" --peers "$roster" $extra \
        > "{{JUST_STATE}}/logs/kv-node${id}.log" 2>&1 &
      echo "$!" > "$pid_file"
    }
    start_node 1 "{{KV_BASE_PORT}}" "--bootstrap"
    start_node 2 "$(( {{KV_BASE_PORT}} + 1 ))" ""
    start_node 3 "$(( {{KV_BASE_PORT}} + 2 ))" ""
    for _ in $(seq 1 100); do
      if "{{ROOT}}/target/debug/rr-admin" --addr "127.0.0.1:$(( {{KV_BASE_PORT}} + 1000 ))" --output json group status --group 1 2>/dev/null | python3 -c 'import json,sys; s=json.load(sys.stdin); raise SystemExit(0 if s.get("leader_node_id") else 1)'; then
        echo "manual kv cluster ready on ports {{KV_BASE_PORT}}..$(( {{KV_BASE_PORT}} + 2 ))"
        exit 0
      fi
      sleep 0.2
    done
    echo "cluster did not become ready; inspect {{JUST_STATE}}/logs" >&2
    exit 1

[group("cluster")]
kv-cluster-status:
    #!/usr/bin/env bash
    set -euo pipefail
    for id in 1 2 3; do
      port=$(( {{KV_BASE_PORT}} + id - 1 ))
      echo "== node $id =="
      "{{ROOT}}/target/debug/rr-admin" --addr "127.0.0.1:$(( port + 1000 ))" --output json group status --group 1
    done

[group("cluster")]
kv-cluster-put KEY="just-manual" VALUE="ok":
    #!/usr/bin/env bash
    set -euo pipefail
    printf 'put %s %s\nget %s\nstatus\nmembers\n' '{{KEY}}' '{{VALUE}}' '{{KEY}}' | \
      "{{ROOT}}/target/debug/rr-admin" repl --addr "127.0.0.1:{{KV_BASE_PORT}}" --group 1

[group("cluster")]
kv-cluster-down:
    #!/usr/bin/env bash
    set -euo pipefail
    for id in 3 2 1; do
      pid_file="{{JUST_STATE}}/pids/kv-node${id}.pid"
      if [[ -e "$pid_file" ]]; then
        pid="$(cat "$pid_file")"
        if kill -0 "$pid" >/dev/null 2>&1; then
          kill -INT "$pid" >/dev/null 2>&1 || true
          for _ in $(seq 1 50); do kill -0 "$pid" >/dev/null 2>&1 || break; sleep 0.1; done
          kill -TERM "$pid" >/dev/null 2>&1 || true
        fi
        rm -f "$pid_file"
      fi
    done

[group("acceptance")]
etcd-up:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v docker >/dev/null || { echo "docker CLI is required" >&2; exit 1; }
    mkdir -p "{{JUST_STATE}}"
    docker_cmd=(docker)
    if [[ -n "{{RR_DOCKER_HOST}}" ]]; then
      docker_cmd=(env "DOCKER_HOST={{RR_DOCKER_HOST}}" docker)
    fi
    "${docker_cmd[@]}" image inspect "{{RR_ETCD_IMAGE}}" >/dev/null 2>&1 || {
      echo "local image {{RR_ETCD_IMAGE}} is required; load/pull it explicitly before this recipe" >&2
      exit 1
    }
    if [[ -n "$("${docker_cmd[@]}" ps --filter "name=^/rr-just-etcd$" --quiet)" ]]; then
      echo "rr-just-etcd is already running"
      exit 0
    fi
    "${docker_cmd[@]}" rm -f rr-just-etcd >/dev/null 2>&1 || true
    "${docker_cmd[@]}" run -d --name rr-just-etcd -p 2379:2379 -p 2380:2380 "{{RR_ETCD_IMAGE}}" \
      /usr/local/bin/etcd --name=node1 --listen-client-urls=http://0.0.0.0:2379 \
      --advertise-client-urls=http://0.0.0.0:2379 --listen-peer-urls=http://0.0.0.0:2380 \
      --initial-advertise-peer-urls=http://0.0.0.0:2380 --initial-cluster=node1=http://0.0.0.0:2380 \
      --initial-cluster-state=new >/dev/null
    echo started > "{{JUST_STATE}}/etcd.container"
    echo "etcd ready at {{RR_ETCD_ENDPOINTS}}"

[group("acceptance")]
etcd-down:
    #!/usr/bin/env bash
    set -euo pipefail
    docker_cmd=(docker)
    if [[ -n "{{RR_DOCKER_HOST}}" ]]; then
      docker_cmd=(env "DOCKER_HOST={{RR_DOCKER_HOST}}" docker)
    fi
    if [[ -e "{{JUST_STATE}}/etcd.container" ]]; then
      "${docker_cmd[@]}" rm -f rr-just-etcd >/dev/null
      rm -f "{{JUST_STATE}}/etcd.container"
    else
      echo "rr-just-etcd was not started by this recipe; leaving it untouched"
    fi

[group("cluster")]
genesis-up:
    #!/usr/bin/env bash
    set -euo pipefail
    just etcd-up
    mkdir -p "{{JUST_STATE}}/pids" "{{JUST_STATE}}/logs"
    cargo build -p kv-store -p rr-admin
    roster="1@127.0.0.1:{{GENESIS_BASE_PORT}},2@127.0.0.1:$(( {{GENESIS_BASE_PORT}} + 1 )),3@127.0.0.1:$(( {{GENESIS_BASE_PORT}} + 2 ))"
    start_node() {
      local id="$1" port="$2"
      local pid_file="{{JUST_STATE}}/pids/genesis-node${id}.pid"
      [[ ! -e "$pid_file" ]] || { echo "genesis node ${id} already has PID state; run just genesis-down first" >&2; exit 1; }
      nohup "{{ROOT}}/target/debug/kv-store" --node-id "$id" --port "$port" --peers "$roster" \
        --formation genesis --cluster-name "{{RR_CLUSTER_NAME}}" --etcd-endpoints "{{RR_ETCD_ENDPOINTS}}" \
        > "{{JUST_STATE}}/logs/genesis-node${id}.log" 2>&1 &
      echo "$!" > "$pid_file"
    }
    start_node 1 "{{GENESIS_BASE_PORT}}"
    start_node 2 "$(( {{GENESIS_BASE_PORT}} + 1 ))"
    start_node 3 "$(( {{GENESIS_BASE_PORT}} + 2 ))"
    for _ in $(seq 1 150); do
      if "{{ROOT}}/target/debug/rr-admin" --addr "127.0.0.1:$(( {{GENESIS_BASE_PORT}} + 1000 ))" --output json group status --group 1 2>/dev/null | python3 -c 'import json,sys; s=json.load(sys.stdin); raise SystemExit(0 if s.get("leader_node_id") else 1)'; then
        echo "homogeneous genesis cluster ready on ports {{GENESIS_BASE_PORT}}..$(( {{GENESIS_BASE_PORT}} + 2 ))"
        exit 0
      fi
      sleep 0.2
    done
    echo "genesis cluster did not become ready; inspect {{JUST_STATE}}/logs" >&2
    exit 1

[group("cluster")]
genesis-status:
    #!/usr/bin/env bash
    set -euo pipefail
    for id in 1 2 3; do
      port=$(( {{GENESIS_BASE_PORT}} + id - 1 ))
      echo "== node $id =="
      "{{ROOT}}/target/debug/rr-admin" --addr "127.0.0.1:$(( port + 1000 ))" --output json group status --group 1
    done

[group("cluster")]
genesis-down:
    #!/usr/bin/env bash
    set -euo pipefail
    for id in 3 2 1; do
      pid_file="{{JUST_STATE}}/pids/genesis-node${id}.pid"
      if [[ -e "$pid_file" ]]; then
        pid="$(cat "$pid_file")"
        if kill -0 "$pid" >/dev/null 2>&1; then
          kill -INT "$pid" >/dev/null 2>&1 || true
          for _ in $(seq 1 50); do kill -0 "$pid" >/dev/null 2>&1 || break; sleep 0.1; done
          kill -TERM "$pid" >/dev/null 2>&1 || true
        fi
        rm -f "$pid_file"
      fi
    done

[group("acceptance")]
test-etcd:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v docker >/dev/null || { echo "docker CLI is required" >&2; exit 1; }
    docker_env=()
    if [[ -n "{{RR_DOCKER_HOST}}" ]]; then
      docker_env=(env "DOCKER_HOST={{RR_DOCKER_HOST}}")
    fi
    "${docker_env[@]}" docker image inspect "{{RR_ETCD_IMAGE}}" >/dev/null 2>&1 || {
      echo "local image {{RR_ETCD_IMAGE}} is required; use etcd-up after loading/pulling it explicitly" >&2
      exit 1
    }
    "${docker_env[@]}" env RR_TEST_ETCD=1 cargo test -p rr-config-etcd --test e2e_real_etcd -- --include-ignored --nocapture
    "${docker_env[@]}" env RR_TEST_ETCD=1 cargo test -p kv-store --test homogeneous_process -- --include-ignored --nocapture

[group("java")]
java-build:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v java >/dev/null || { echo "java is required for the Java SDK" >&2; exit 1; }
    [[ -x "{{JAVA_DIR}}/gradlew" ]] || { echo "{{JAVA_DIR}}/gradlew is missing or not executable" >&2; exit 1; }
    cd "{{JAVA_DIR}}"
    ./gradlew build

[group("java")]
java-test:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v java >/dev/null || { echo "java is required for the Java SDK" >&2; exit 1; }
    [[ -x "{{JAVA_DIR}}/gradlew" ]] || { echo "{{JAVA_DIR}}/gradlew is missing or not executable" >&2; exit 1; }
    cd "{{JAVA_DIR}}"
    ./gradlew test

[group("java")]
java-integration-test:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v java >/dev/null || { echo "java is required for the Java SDK" >&2; exit 1; }
    [[ -x "{{JAVA_DIR}}/gradlew" ]] || { echo "{{JAVA_DIR}}/gradlew is missing or not executable" >&2; exit 1; }
    command -v docker >/dev/null || { echo "docker CLI is required for integration tests (testcontainers)" >&2; exit 1; }
    docker_env=()
    if [[ -n "{{RR_DOCKER_HOST}}" ]]; then
      docker_env=(env "DOCKER_HOST={{RR_DOCKER_HOST}}")
    fi
    "${docker_env[@]}" docker info >/dev/null 2>&1 || {
      echo "docker daemon is not reachable; start it or set RR_DOCKER_HOST" >&2
      exit 1
    }
    cd "{{JAVA_DIR}}"
    "${docker_env[@]}" env RR_TEST_DOCKER=1 ./gradlew integrationTest

[group("java")]
java-javadoc:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v java >/dev/null || { echo "java is required for the Java SDK" >&2; exit 1; }
    [[ -x "{{JAVA_DIR}}/gradlew" ]] || { echo "{{JAVA_DIR}}/gradlew is missing or not executable" >&2; exit 1; }
    cd "{{JAVA_DIR}}"
    ./gradlew javadoc

[group("java")]
java-publish-local:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v java >/dev/null || { echo "java is required for the Java SDK" >&2; exit 1; }
    [[ -x "{{JAVA_DIR}}/gradlew" ]] || { echo "{{JAVA_DIR}}/gradlew is missing or not executable" >&2; exit 1; }
    cd "{{JAVA_DIR}}"
    ./gradlew publishToMavenLocal
    echo "published io.github.gardenias:rr-client to the local Maven repository only (~/.m2)"

[group("java")]
java-clean:
    #!/usr/bin/env bash
    set -euo pipefail
    [[ -x "{{JAVA_DIR}}/gradlew" ]] || { echo "{{JAVA_DIR}}/gradlew is missing or not executable" >&2; exit 1; }
    cd "{{JAVA_DIR}}"
    ./gradlew clean

# ---------------------------------------------------------------------------
# Quality gates — local replacement for the disabled nightly CI workflows.
# Results are archived by scripts/quality-gate.sh under target/quality-runs/.
# ---------------------------------------------------------------------------

FUZZ_SECONDS := env_var_or_default("RR_FUZZ_SECONDS", "300")
SOAK_SMOKE_DURATION := env_var_or_default("RR_SOAK_SMOKE_DURATION", "180")
TLA_VERSION := "1.7.4"
TLA_JAR := ROOT / "target/tools/tla2tools.jar"

[group("gates")]
gate-invariants:
    ./scripts/check-invariants.sh

[group("gates")]
metrics-check:
    ./scripts/check-metrics-docs.sh

[group("gates")]
gate-test:
    cargo test --workspace

[group("gates")]
gate-crash:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build -p crash-tests --features rocks
    RR_CRASH_NODE_BIN="{{ROOT}}/target/debug/rr-crash-node" \
      cargo test -p crash-tests --features rocks -- --test-threads=1 --nocapture

[group("gates")]
gate-fuzz:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo fuzz --version >/dev/null 2>&1 || {
      echo "cargo-fuzz is required: cargo install cargo-fuzz --locked --version 0.13.2" >&2
      exit 1
    }
    rustup toolchain list | grep -q '^nightly' || {
      echo "nightly toolchain is required: rustup toolchain install nightly" >&2
      exit 1
    }
    targets=(
      fuzz_framed_entry_decode fuzz_framed_snapshot_decode
      fuzz_apply_outcome_decode fuzz_client_request_proto
      fuzz_framed_entry_roundtrip fuzz_apply_outcome_roundtrip
      fuzz_batch_frame_decode fuzz_batch_frame_roundtrip
    )
    for t in "${targets[@]}"; do
      echo "== fuzz $t ({{FUZZ_SECONDS}}s) =="
      cargo +nightly fuzz run "$t" --fuzz-dir fuzz -- -max_total_time={{FUZZ_SECONDS}} -error_exitcode=42
    done

[group("gates")]
gate-formal:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v java >/dev/null || { echo "java is required for the TLC model checker" >&2; exit 1; }
    if [[ ! -f "{{TLA_JAR}}" ]]; then
      mkdir -p "$(dirname "{{TLA_JAR}}")"
      echo "downloading pinned tla2tools {{TLA_VERSION}} (one-off)"
      curl -sSL -o "{{TLA_JAR}}" \
        "https://github.com/tlaplus/tlaplus/releases/download/v{{TLA_VERSION}}/tla2tools.jar"
    fi
    cd "{{ROOT}}/formal/tla"
    for spec in MembershipGuard OffsetExactlyOnce OutputWatermark; do
      java -XX:+UseParallelGC -jar "{{TLA_JAR}}" -deadlock "$spec.tla"
    done
    for cfg in RoutingTableSwitch RoutingTableSwitchIncremental; do
      java -XX:+UseParallelGC -jar "{{TLA_JAR}}" -deadlock -config "$cfg.cfg" RoutingTableSwitch.tla
    done

[group("gates")]
bench-save-baseline:
    cargo bench --workspace --features rr-benches/rocks -- --save-baseline main

[group("gates")]
bench-gate:
    #!/usr/bin/env bash
    set -euo pipefail
    [[ -d "{{ROOT}}/target/criterion" ]] || {
      echo "no criterion baseline found; run 'just bench-save-baseline' first" >&2
      exit 1
    }
    mkdir -p "{{ROOT}}/target/quality-runs"
    out="{{ROOT}}/target/quality-runs/bench-$(date +%Y%m%d-%H%M%S).txt"
    cargo bench --workspace --features rr-benches/rocks -- --baseline main | tee "$out"
    ./scripts/bench-compare.sh "$out"

[group("gates")]
soak-smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -n "{{RR_DOCKER_HOST}}" ]]; then export DOCKER_HOST="{{RR_DOCKER_HOST}}"; fi
    command -v docker >/dev/null || { echo "docker CLI is required" >&2; exit 1; }
    docker info >/dev/null 2>&1 || { echo "docker daemon is not reachable; set RR_DOCKER_HOST" >&2; exit 1; }
    docker image inspect rr-lab:dev >/dev/null 2>&1 || {
      echo "rr-lab:dev image missing; build it first:" >&2
      echo "  docker compose -f deploy/lab/docker-compose.yml build" >&2
      exit 1
    }
    SOAK_DURATION={{SOAK_SMOKE_DURATION}} SOAK_FAULT_MEAN_SECS=45 ./scripts/soak.sh

[group("gates")]
nightly:
    ./scripts/quality-gate.sh nightly

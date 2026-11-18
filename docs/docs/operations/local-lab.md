# 本地实验环境 / Local Lab (OrbStack / Docker)

> **仅本地实验环境。** 基础 `deploy/lab/` 编排无 TLS、无认证，kv-store 以 `allow_insecure` 运行。禁止用于真实部署。
> 可选 Console overlay 只在 Browser→Console 边界增加 HTTPS 和 viewer token，不改变节点面的安全设置。
>
> **LOCAL LAB ONLY.** The base `deploy/lab/` stack runs without TLS or authentication, with kv-store in insecure mode.
> Never reuse it for a real deployment. The optional Console overlay adds HTTPS and viewer authentication only at the
> Browser→Console boundary; it does not change node-side security.

`deploy/lab/` 提供 3 节点 kv-store、Redpanda（Kafka 协议）、Prometheus/Grafana，以及可选只读 Cluster Admin
Console，用于容器化、压力、混沌和稳定性长跑验证。

`deploy/lab/` provides a three-node kv-store cluster, Redpanda (Kafka API), Prometheus/Grafana, and an optional
read-only Cluster Admin Console for container, stress, chaos, and soak verification.

## 环境准备 / Prerequisites

- OrbStack 或 Docker Desktop；若当前 context 指向 Docker Desktop，可执行 `docker context use orbstack`。<br />
  OrbStack or Docker Desktop; if the active context points to Docker Desktop, run `docker context use orbstack`.
- 宿主机构建 `rr-admin`：`cargo build --release -p rr-admin`。<br />
  Build the host-side admin CLI with `cargo build --release -p rr-admin`.
- 构建 lab 镜像前生成 Console 静态导出：`just console-build`（需要 `just` 和 `pnpm`）。Dockerfile 会复制
  `crates/rr-admin/console/out`，运行镜像不需要 Node.js。<br />
  Generate the Console static export before building the lab image with `just console-build` (requires `just` and
  `pnpm`). The Dockerfile copies `crates/rr-admin/console/out`; Node.js is not needed in the runtime image.
- 端口：client+raft `51101-51103`，Admin `52101-52103`，HTTP/metrics `9111-9113`，Kafka `19092`，Prometheus `9090`，
  Grafana `3000`，混沌 toxiproxy API `18474`；可选 Console 为 `127.0.0.1:8080`（HTTPS）。<br />
  Ports: client+raft `51101-51103`, Admin `52101-52103`, HTTP/metrics `9111-9113`, Kafka `19092`, Prometheus `9090`,
  Grafana `3000`, and chaos toxiproxy API `18474`; the optional Console uses `127.0.0.1:8080` (HTTPS).

## 快速开始 / Quickstart

```bash
just console-build
docker compose -f deploy/lab/docker-compose.yml up -d --build

docker exec kv-1 rr-admin --addr 127.0.0.1:52001 group status --group 1

./target/release/rr-admin repl --addr 127.0.0.1:51101 --group 1
# rr> put foo bar
# rr> get foo
```

Grafana 为 `http://localhost:3000`（admin/admin），看板是 `Rr Cluster Overview`；Prometheus 为 `http://localhost:9090`。
宿主机的 REPL 写必须发往 leader 对应的已发布端口，因为 redirect 返回的是容器网络主机名。

Grafana is at `http://localhost:3000` (admin/admin) with the `Rr Cluster Overview` dashboard; Prometheus is at
`http://localhost:9090`. Host-side REPL writes must target the published port for the current leader because redirects
contain container-network hostnames.

## 可选只读 Console / Optional Read-only Console

`docker-compose.console.yml` 在基础栈上增加 `rr-lab-console`，复用 `rr-lab:dev` 中的 `rr-admin console` 和静态导出；
不需要独立 Node.js 生产服务。完成上述 `just console-build` 后，设置四个非空环境变量并启动 overlay：

`docker-compose.console.yml` adds `rr-lab-console` to the base stack, reusing `rr-admin console` and the static export
from `rr-lab:dev`; no standalone Node.js production server is needed. After `just console-build`, set these four
non-empty environment variables and start the overlay:

```bash
export RR_CONSOLE_TOKEN='local-viewer-token'
export RR_CONSOLE_TLS_CERT=/absolute/path/to/console.crt
export RR_CONSOLE_TLS_KEY=/absolute/path/to/console.key
export RR_CONSOLE_CA_CERT=/absolute/path/to/ca.crt

docker compose -f deploy/lab/docker-compose.yml \
  -f deploy/lab/docker-compose.console.yml up -d --build
```

将路径替换为实际文件。Console 证书的 SAN 必须同时覆盖 `localhost` 和 `127.0.0.1`；CA 必须是其签发 CA。三个
证书/密钥文件必须可由容器 UID `10001` 读取。CA 也用于容器 HTTPS healthcheck。`RR_CONSOLE_TOKEN` 是浏览器
viewer token，不是节点 Admin token；基础 lab 的节点 AdminService 仍使用无认证明文连接。

Replace the paths with actual files. The Console certificate SANs must cover both `localhost` and `127.0.0.1`, and the
CA must have signed that certificate. All three certificate/key files must be readable by container UID `10001`. The CA
is also used by the container HTTPS healthcheck. `RR_CONSOLE_TOKEN` is the browser viewer token, not a node Admin token;
the base lab's node AdminService still uses plaintext without authentication.

信任该 CA 后打开 **`https://localhost:8080`**，用 viewer token 登录。端口仅绑定宿主机 loopback。BFF 通过 lab
网内 Admin gRPC 地址 `kv-1:52001`、`kv-2:52001`、`kv-3:52001` 采集节点、组和拓扑等 Admin-backed 只读数据。
当前 `kv-store` 未配置 Console 节点描述符，因此没有受信 `observability_addr`；`/health`、`/ready` 和 `/metrics`
采集以及 `CONSOLE_MEMORY` 实时趋势会按能力降级，仅开放 9100 端口不会启用这条路径。完整实时观测要求节点另行
提供完整描述符，本 overlay 不能用于验收该能力。overlay 也未配置 etcd discovery 或 Prometheus 历史，现有
Prometheus/Grafana 仍独立使用。

After trusting the CA, open **`https://localhost:8080`** and log in with the viewer token. The port is bound only to
host loopback. The BFF collects Admin-backed read-only node, group, and topology data through the in-network Admin gRPC
addresses `kv-1:52001`, `kv-2:52001`, and `kv-3:52001`. The current `kv-store` does not configure a Console node
descriptor and has no trusted `observability_addr`; `/health`, `/ready`, and `/metrics` collection and
`CONSOLE_MEMORY` live trends degrade by capability. Publishing port 9100 alone does not enable this path. Full live
observability requires a complete node descriptor, so this overlay cannot validate that capability. The overlay also
configures neither etcd discovery nor Prometheus history; the existing Prometheus/Grafana stack remains independent.

Console 只有 viewer 能力：reconcile POST 也只是只读刷新，不提供 membership 修改、强制选主、TriggerSnapshot、
DrainNode、offset reset 或进程重启。受控写操作仍使用 Admin CLI；停止 Console 不影响 Raft 节点。拆除含 Console
的完整栈时，同时使用两个 compose 文件：

The Console is viewer-only: reconcile POST is only a read-only refresh, with no membership changes, forced leadership,
TriggerSnapshot, DrainNode, offset reset, or process restart. Continue to use the Admin CLI for controlled writes;
stopping the Console does not affect Raft nodes. To tear down the full stack including Console, use both compose files:

```bash
docker compose -f deploy/lab/docker-compose.yml \
  -f deploy/lab/docker-compose.console.yml down -v
```

## 压力测试 / Stress

负载必须在 lab 网络内运行，以便解析 leader redirect 的地址：

Run load inside the lab network so leader-redirect addresses resolve:

```bash
docker compose -f deploy/lab/docker-compose.yml --profile bench run --rm bench

docker compose -f deploy/lab/docker-compose.yml --profile bench run --rm bench \
  --cluster-addrs=kv-1:51001,kv-2:51001,kv-3:51001 \
  --scenario=s2 --concurrency 1,10,50,100 --duration-secs 20
```

## Kafka 输入与恰好一次验证 / Kafka Input and Exactly-Once Verification

```bash
docker compose -f deploy/lab/docker-compose.yml --profile produce run --rm --name rr-lab-producer producer \
  --brokers redpanda:9092 --topic input_1 --rate 1000 --total 10000

./target/release/rr-admin repl --addr 127.0.0.1:51101 --group 1
# rr> get lab:counter  → 10000
# rr> get lab:seq:1    → v1
```

判据是 `lab:counter` 恰好等于自增命令数，并抽查 `lab:seq:<i>` 为 `v<i>`。Kafka at-least-once 投递加 offset →
`IdempotencyKey` 去重，提供恰好一次 apply。详见 [Kafka 输入指南](../guide/kafka-input.md) 和
[幂等指南](../guide/idempotency.md)。

The counter must exactly equal the number of increment commands, and sampled `lab:seq:<i>` keys must contain `v<i>`.
Kafka at-least-once delivery plus offset-to-`IdempotencyKey` deduplication provides exactly-once apply. See the
[Kafka Input](../guide/kafka-input.md) and [Idempotency](../guide/idempotency.md) guides.

## 混沌测试 / Chaos

```bash
docker compose -f deploy/lab/docker-compose.yml -f deploy/lab/docker-compose.chaos.yml up -d --build

bash deploy/lab/chaos/kill-leader.sh
bash deploy/lab/chaos/run-all.sh
```

`run-all.sh` 覆盖 kill-leader、pause-follower、partition-leader、latency-spike、packet-loss、state-wipe-restart、
rolling-restart。每个脚本自带新 leader 时限、写探针、读回和追平断言。

`run-all.sh` covers kill-leader, pause-follower, partition-leader, latency-spike, packet-loss, state-wipe-restart, and
rolling-restart. Each script asserts leadership timing, write probes, read-back, and catch-up as applicable.

当前心跳 50ms、选举超时 150–300ms，硬编码于 `rr-raft/src/raft_node.rs`。接近或超过 150ms 的链路会反复选举，因此
latency-spike 使用 40ms±10ms，而不是 200ms。当前版本不适合高延迟跨地域部署。

The current 50ms heartbeat and 150–300ms election timeout are hardcoded in `rr-raft/src/raft_node.rs`. Links near or
above 150ms can cause repeated elections, so latency-spike uses 40ms±10ms rather than 200ms. The current version is not
suitable for high-latency cross-region deployment.

## 稳定性长跑 / Soak

```bash
./deploy/lab/soak.sh
./deploy/lab/soak.sh 600
```

默认运行 2 小时，报告写入 `deploy/lab/reports/soak-<ts>.md`，同时保存 `.samples.csv` 和 bench 原始日志。通过标准：客户端
零错误、意外选主不超过 1 次、RSS 无单调增长、p99 无显著漂移。

The default run is two hours. Reports go to `deploy/lab/reports/soak-<ts>.md`, with `.samples.csv` and raw bench logs.
Pass criteria: zero client errors, at most one unexpected election, no monotonic RSS growth, and no significant p99
drift.

## 语义与限制 / Semantics and Limitations

- 默认 **MemStorage** 在容器重启后丢失日志、vote 和内存快照，不是持久化。存活 leader 可通过日志或 InstallSnapshot 让节点
  追平。Rocks 模式则把日志和快照落盘，见 [持久化与恢复](../architecture/durability.md)。<br />
  The default **MemStorage** loses logs, votes, and in-memory snapshots on restart; it is not persistence. A surviving
  leader can catch the node up through logs or InstallSnapshot. Rocks mode persists logs and snapshots; see
  [Durability & Recovery](../architecture/durability.md).
- lab 全部 `allow_insecure`，只绕过 G1–G6；G7 TCP loopback 门始终执行。混沌模式仍使用 gRPC。实验 TCP transport 的
  linearizable redirect 在并发下有已知不稳定，见 [benchmarks](../reference/benchmarks.md)。<br />
  The lab uses `allow_insecure`, bypassing only G1–G6; the G7 TCP loopback gate always applies. Chaos mode still uses
  gRPC. The experimental TCP transport has known concurrent linearizable-redirect instability; see
  [benchmarks](../reference/benchmarks.md).
- 同 ID 空节点追平存在已知 openraft 进度问题。`state-wipe-restart.sh` 的 legacy 流程是：移出 voters → 添加 learner → 45
  秒无进展则重启当前 leader 以重建进度跟踪 → 追平后恢复 voter。不要把该 workaround 当作 managed P1 副本替换支持。<br />
  A wiped same-ID node can hit a known openraft progress-tracking issue. The legacy `state-wipe-restart.sh` flow removes
  it from voters, adds a learner, restarts the current leader if no progress occurs within 45 seconds, then restores
  voter membership after catch-up. Do not treat this workaround as managed-P1 replica-replacement support.
- 黑洞链路恢复后依赖 keepalive 清理陈旧 H2 通道；排查时同时检查 `rr_channel_reconnects_total`。<br />
  Healing a black-holed link relies on keepalive to remove stale H2 channels; inspect `rr_channel_reconnects_total`
  during diagnosis.

## 目录 / Layout

- `Dockerfile`：`rr-lab:dev`，包含 kv-store[rdkafka]、rr-admin、rr-perf-bench、rr-lab-producer 和 Console 静态导出。<br />
  Builds `rr-lab:dev` with kv-store[rdkafka], rr-admin, rr-perf-bench, rr-lab-producer, and the Console static export.
- `docker-compose.yml`、`docker-compose.chaos.yml`、`toxiproxy.json`：基础与混沌编排。<br />
  Base and chaos orchestration.
- `docker-compose.console.yml`：可选只读 Console overlay，启用 HTTPS、viewer token 和 `127.0.0.1:8080`。<br />
  Optional read-only Console overlay with HTTPS, a viewer token, and `127.0.0.1:8080`.
- `prometheus.yml`、`grafana/provisioning/`：复用 `deploy/observability/` 的看板与规则。<br />
  Reuses dashboards and rules from `deploy/observability/`.
- `chaos/`：7 个场景、`lib.sh` 和 `run-all.sh`。<br />
  Seven scenarios, `lib.sh`, and `run-all.sh`.
- `soak.sh`、`reports/`：长跑入口与输出。<br />
  Soak entry point and outputs.

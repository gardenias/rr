# 本地实验环境（OrbStack / Docker）/ Local Lab

> **仅本地实验环境 / LOCAL LAB ONLY** — `deploy/lab/` 的全部编排无 TLS、无认证，
> kv-store 以 allow_insecure 运行。禁止用于任何真实部署。
> Everything under `deploy/lab/` runs without TLS/auth in insecure mode. Never reuse for real deployments.

`deploy/lab/` 提供一套可复现的本地实验环境：**3 节点 kv-store 集群（真实 KV 语义）+ Redpanda（Kafka 协议）命令输入 + Prometheus/Grafana 可观测性**，用于四个维度的验证：容器化部署、压力、混沌、稳定性长跑。

`deploy/lab/` is a reproducible local lab: a **3-node kv-store cluster (real KV semantics) + Redpanda (Kafka API) command input + Prometheus/Grafana**, used for four verification tracks: containerized deployment, stress, chaos, and hours-long soak.

## 环境准备 / Prerequisites

- OrbStack（或 Docker Desktop）；若当前 docker context 指向 Docker Desktop：`docker context use orbstack`
- 仓库根目录构建过 `rr-admin`（宿主机验证用）：`cargo build --release -p rr-admin`
- 宿主机端口规划：lab 使用 `51101-51103`（client+raft）、`52101-52103`（admin）、`9111-9113`（HTTP/metrics）、`19092`（Kafka）、`9090`（Prometheus）、`3000`（Grafana）、`18474`（toxiproxy API，仅混沌模式）。注意避开本机 51001-51003（可能被其他栈占用）。

## 快速开始 / Quickstart

```bash
# 启动全栈（首次构建数分钟：rdkafka 编译捆绑的 librdkafka）
docker compose -f deploy/lab/docker-compose.yml up -d --build

# 集群状态（三个节点应一致）
docker exec kv-1 rr-admin --addr 127.0.0.1:52001 group status --group 1

# 宿主机读写（真实 KV 语义；写必须发往 leader 的已发布端口）
./target/release/rr-admin repl --addr 127.0.0.1:51101 --group 1
# rr> put foo bar / get foo

# Grafana: http://localhost:3000 (admin/admin) → "rr Cluster Overview"
# Prometheus: http://localhost:9090
```

## 四个测试维度 / The four tracks

### 1. 压力 / Stress

负载在 **lab 网络内**运行（leader-redirect 返回网内主机名地址，宿主机不可解析）：

Load runs **inside** the lab network (leader redirects carry in-network hostnames that don't resolve from the host):

```bash
# 完整场景（S1 串行写 + S2 并发扫描 + S3 读路径）
docker compose -f deploy/lab/docker-compose.yml --profile bench run --rm bench

# 自定义
docker compose -f deploy/lab/docker-compose.yml --profile bench run --rm bench \
  --cluster-addrs=kv-1:51001,kv-2:51001,kv-3:51001 \
  --scenario=s2 --concurrency 1,10,50,100 --duration-secs 20
```

### 2. Kafka 输入链路与 exactly-once 验证 / Kafka input & exactly-once

```bash
# 启动命令生产者（1 万条自增 + 1 千条 put）
docker compose -f deploy/lab/docker-compose.yml --profile produce run --name rr-lab-producer producer \
  --brokers redpanda:9092 --topic input_1 --rate 1000 --total 10000

# 判据 A（exactly-once apply）：lab:counter 终值必须**恰好**等于自增命令条数
./target/release/rr-admin repl --addr 127.0.0.1:51101 --group 1   # rr> get lab:counter → 10000

# 判据 B（数据完整）：抽查 lab:seq:<i> 读回 v<i>
```

原理：Kafka at-least-once 投递 + 框架幂等层（offset → `IdempotencyKey` 去重）= `Service::handle` 恰好执行一次。详见 [Kafka 输入指南](../guide/kafka-input.md) 与 [幂等指南](../guide/idempotency.md)。

### 3. 混沌 / Chaos

```bash
# 以混沌模式启动（toxiproxy 介入节点间流量）
docker compose -f deploy/lab/docker-compose.yml -f deploy/lab/docker-compose.chaos.yml up -d --build

# 单个场景 / single scenario
bash deploy/lab/chaos/kill-leader.sh

# 全套（kill-leader / pause-follower / partition-leader / latency-spike /
#       packet-loss / state-wipe-restart / rolling-restart）
bash deploy/lab/chaos/run-all.sh
```

每个脚本自带判据（新 leader 时限、写探针、读回一致性、追平检查、错误率）。已知校准点：

**校准要点 / calibration**：默认心跳 50ms、选举超时 150-300ms（当前版本硬编码于 `rr-raft/src/raft_node.rs`，不可调）。节点间链路延迟接近或超过 ~150ms 选举下限时会反复重新选举——`latency-spike.sh` 因此使用 40ms±10ms（远高于容器内正常 RTT，但低于选举下限）而不是 200ms。跨地域/高延迟部署在当前版本不可行，需等待选举参数可配置化。

### 4. 稳定性长跑 / Soak

```bash
# 默认 2 小时；采样写 deploy/lab/reports/soak-<ts>.md（+ .samples.csv + bench 原始日志）
./deploy/lab/soak.sh          # 7200s
./deploy/lab/soak.sh 600      # 10 分钟快速版
```

通过标准：客户端零错误；意外选主 ≤1 次（term 增量）；各节点 RSS 无单调增长；p99 无显著漂移。

## 已知语义与限制 / Known semantics & limitations

- **MemStorage**：容器重启即丢全量状态，重进集群靠 leader 的 `InstallSnapshot` 追平（`chaos/state-wipe-restart.sh` 验证）。这不是数据持久化。
- **快照仅存内存**：详见 [持久化与恢复](../architecture/durability.md)。
- **实验性 TCP raft 传输**：混沌模式下仍走 gRPC；TCP 传输的 linearizable 重定向在并发下有已知不稳定（见 [benchmarks](../reference/benchmarks.md)）。
- lab 全部 `allow_insecure`：G7 门禁之外的 TLS/认证检查全部关闭。

### 已验证的故障恢复要点 / Validated recovery notes

- **被擦除的 rocks 节点（同 id）的追平需要一次换主**：openraft 0.10.0-alpha.34 下，leader 对被清空重建的 voter 的复制进度（`matching`）是陈旧的，不会自动驱动追平。已验证的恢复流程（`chaos/state-wipe-restart.sh` 编码并断言）：`change-membership` 将其移出 voters → `add-learner` 重新加入 → 若 45s 内无追平进展则**重启当前 leader**（换主会重建进度跟踪器，追平立即进行）→ `change-membership` 恢复为 voter。生产替换节点建议走同样流程。默认 mem 节点重启为空属于预期行为，存活 leader 会重置复制进度并从头追平，无需换主；该日志回退恢复不用于 rocks 路径。
- **选举超时是数据中心级假设**：心跳 50ms / 选举 150-300ms 硬编码；链路延迟 ≥150ms 会持续重选（混沌套件以 40ms 校准验证稳定性）。
- **黑洞链路的连接必须由 keepalive 清除**：本轮修复前，网络分区恢复后陈旧 H2 通道会让分裂状态持续到进程重启。

---

- **Wiped-node (same id) catch-up needs a leadership change**: with openraft 0.10.0-alpha.34 the incumbent leader's replication progress (`matching`) for a wiped-and-restarted voter is stale and does not drive catch-up by itself. Validated recovery flow (encoded + asserted by `chaos/state-wipe-restart.sh`): `change-membership` it out of voters → `add-learner` → if no catch-up progress within 45s, **restart the current leader** (the tracker rebuilds and catch-up proceeds immediately) → `change-membership` back to voter. Use the same flow when replacing nodes in production.
- **Election timeouts assume datacenter links**: heartbeat 50ms / election 150-300ms are hardcoded; links at ≥150ms will re-elect continuously (the chaos suite validates stability at a calibrated 40ms).
- **Black-holed links need keepalive to tear down dead channels**: before this round's fix, a healed partition could leave stale H2 channels wedged until process restart.

## 目录 / Layout

- `Dockerfile` — `rr-lab:dev` 镜像（kv-store[rdkafka] + rr-admin + rr-perf-bench + rr-lab-producer），BuildKit 缓存挂载加速增量重建
- `docker-compose.yml` / `docker-compose.chaos.yml` / `toxiproxy.json`
- `prometheus.yml` + `grafana/provisioning/`（复用 `deploy/observability/` 的看板与规则）
- `chaos/`（7 个场景脚本 + `lib.sh` 断言库 + `run-all.sh`）
- `soak.sh` + `reports/`

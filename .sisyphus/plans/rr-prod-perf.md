# rr 生产性能测试方案(rr-prod-perf)

> **状态**:设计定稿(2026-09-10);方案 A(rr-perf-bench 纯加法扩展)与 kv-store 改动已获用户
> 批准;动工前查实 G8(raft log 持久化未接线)并增补 M0 前置里程碑;**M0 已落地
> (2026-09-10,ADR A-0016 → implemented);M1 已落地(2026-09-10,deploy/perf/ +
> kv-store TLS 参数面,selfcheck 8/8 实测绿);M2 已落地(2026-09-10,openloop 引擎 +
> TLS client,本地 + perf-lab mTLS 冒烟绿,S1/S2/S3 schema 回归干净);M3 已落地
> (2026-09-10,--prom-url 窗口采集 + prod-perf.sh 一键矩阵 + 三 overlay + rr-admin
> TLS,全量 smoke 全档绿),M4(首轮完整矩阵实测 + 基线归档 + A-0010 重判)待动工**
> **依据**:用户指示——"按真实生产环境要求设计集群;先设计性能测试方案;已有集成性能测试
> base 结合起来看,在此基础上调整;**局部性能测试脚本暂时都不动**,要搭建的是一个生产性能
> 测试工具"。
> **既有资产**:R-7 基线(`docs/perf/baselines/v0.2.0-baseline.md`)、R-8 soak(`scripts/soak.sh`)、
> `tools/rr-perf-bench`、`deploy/lab`、`deploy/observability`、Helm/K8s 生产制品。
> **基线**:HEAD=`ae0fc43`(v0.2.0 tag 后)。

---

## 1. 目的与原则

### 1.1 目的

回答 R-7 基线**不能**回答的问题:**rr 以生产形态部署时,性能包线(envelope)是多少?**
具体而言:

1. 生产配置(TLS + mTLS + bearer auth、rocks/strict、**durable raft log**、容器化、资源限额、
   独立负载机)下的吞吐饱和点与各饱和度下的尾延迟;
2. 安全层的性能成本(TLS/mTLS/auth A/B 对比);
3. 混合负载、热点 key、线性一致读、Kafka 摄入路径的生产级表现;
4. 长时运行(steady-state soak)与快照压力下的延迟稳定性(时间序列,非聚合点);
5. 形成**可复跑、可对比、可存档**的生产性能基线,作为后续版本性能回归与容量规划的依据;
6. **以生产形态重判 ADR A-0010 前置条件 2**:R-7 基线的写路径未包含 raft log 存储
   (当时被测二进制的 raft log 为内存,见 G8),"存储不是绑定约束"的结论须在
   rocks-log + fsync 进回路后重测重判。

### 1.2 原则

1. **测生产形态,不测开发形态**。被测集群必须能通过 `RrNode::new` 的 G1–G7 安全门
   (即 TLS + mTLS + admin/client token,`RR_ALLOW_INSECURE` 不存在);存储必须
   rocks/strict;部署制品以 Helm/compose 生产形态为准。
2. **客户端视角度量 + 无协调遗漏(coordinated omission)**。延迟以"计划发起时刻 → 完成"
   计(open-loop),不是"实际发起 → 完成"(closed-loop);两者同测对比,量化闭合回路
   对尾延迟的低估。
3. **负载机与被测集群资源隔离**。负载发生器不得与被测节点争抢 CPU(R-7 基线的已知污染:
   客户端与 3 节点同台 11 核机器,饱和段三方合计已顶满)。
4. **复用优先,加法不改法**。负载引擎、HDR 度量、`--json-out`、soak oracle 复用
   `rr-perf-bench` 既有实现并以**纯加法**扩展;`scripts/perf-baseline.sh`、`scripts/soak.sh`、
   `deploy/lab/`、CI bench job **逐字节不动**(R-7/R-8 基线可比性不受影响)。
5. **一切产出机器可读**:每次运行落 raw JSON + Prometheus 快照 + 环境 meta(git SHA /
   rustc / OS / 拓扑 / 安全配置),报告从原始数据生成,禁止手抄数字。
6. **数值先测量后承诺**:首轮运行回填基线表;SLO 判据模板先行,具体阈值以首轮数据定,
   不凭空立 SLA。

## 2. 现状盘点与差距分析

### 2.1 既有性能测试 base(可复用资产)

| 资产 | 能力 | 复用方式 |
|---|---|---|
| `tools/rr-perf-bench` | S1 串行写(closed-loop 1 in-flight);S2 reactive pipeline 并发扫描(closed-loop);S3 local/linearizable 读扫描;`--scenario soak`(open-loop 定率 keyed 幂等写 + lin-read 探针 + ack-log oracle);HDR histogram(1µs–30s,3 位有效数字);本地进程集群(1/3/5 节点,mem/rocks × 3 档 durability)或 `--cluster-addrs` 外接集群;`--json-out` 机器可读报告 | **负载引擎与度量核心,加法扩展**(见 §5) |
| `scripts/perf-baseline.sh` | R-7 基线矩阵采集 + CPU 采样 | 不动;其职责(开发态回归基线)与本方案(生产态包线)正交 |
| `scripts/soak.sh` + `deploy/lab/` | 3 节点 kv-store 容器 + Redpanda + Prom/Grafana + Toxiproxy;故障循环 + 三件套 oracle | 不动;R-8 的可靠性 soak。本方案新建 `deploy/perf/`(§3),**编排模式(网内负载、healthcheck 门、报告目录)从 soak.sh 借鉴但不改它** |
| `deploy/observability/` | drop-in Prometheus/Grafana(recording rules 已按延迟预算分层) | perf-lab 直接挂载复用 |
| `deploy/helm/rr` + `deploy/k8s/` | 生产制品:3 副本、rocks/strict、TLS+auth 默认强制、PDB、资源限额 | Tier-1 拓扑(§3.3)的被测形态 |
| `dev-certs/` + `scripts/gen-dev-certs.sh` | 自签 CA + 节点证书 | perf-lab 证书来源(按 lab hostname 重新生成,见 §3.2) |
| `docs/perf/baselines/v0.2.0-baseline.md` | M3 Pro loopback ~34k TPS 饱和、CPU 约束判定 | 对照组:生产形态数字与它并排读,量化"生产税" |

### 2.2 差距分析(为什么 R-7 基线不等于生产性能)

| # | 差距 | 影响 |
|---|---|---|
| G1 | **负载机与集群同机混部**,无任何资源隔离 | 饱和点被客户端污染,~34k TPS 是"整机剩余算力"而非集群能力 |
| G2 | **无 TLS/mTLS/token**(`RR_ALLOW_INSECURE`) | 安全层握手/加解密/鉴权成本完全未测;生产必须开启 |
| G3 | **closed-loop 负载模型**(S1/S2/S3) | 协调遗漏系统性低估尾延迟;无法回答"在 X TPS 目标下 p99 是多少" |
| G4 | **单 partition、单客户端、纯写或纯读** | 无混合读写、无热点 key、无多客户端扇出、无多 group 扩展性数据 |
| G5 | **20s 短窗 + 聚合百分位** | 快照触发(默认 50k 条,饱和段 ~1.5s 一次)、compaction、leader 切换造成的秒级毛刺不可见;无时间序列 |
| G6 | **Kafka 摄入路径未测** | 生产主入口(ingestor → `write_batch`)的端到端提交速率/滞后无数据 |
| G7 | **客户端侧单一视角** | 无法把客户端毛刺归因到服务端环节(proposal/apply/replication RTT) |
| G8 | **raft log 持久化未接线(2026-09-10 动工前查实)**:所有生产二进制(stock `rr-server`、`kv-store`)的 `start_group` → `RaftNode::new_with_factory` → `RrLogStorage`(内存 log,`raft_node.rs:85`);`start_group_rocks`/`RocksLogStorage` 生产路径零调用,且其硬编码 `NoopNetworkFactory`(`group_manager.rs:1081`)——"rocks log + 复制"的组合在生产代码中**不存在**,唯一双开组装在 `tests/crash` 的 rr-crash-node | (a) R-7 基线写路径最热的一段(raft log append + IoBatcher + fsync)**不在测量回路内**,"fsync 不是瓶颈"仅覆盖 SM 侧,A-0010 前置判据 2 的证据不充分;(b) 默认部署形态(Helm/K8s 跑 stock rr-server)全集群断电即丢未快照日志——strict 耐久契约在默认形态不成立,**这是生产正确性缺口,不止 perf 缺口**(复制本身已开启:`new_with_network` → `GrpcNetworkFactory`,AppendEntries 为真) |

## 3. 生产基准集群设计(perf-lab)

### 3.1 拓扑总览(Tier-0,默认形态)

新建 `deploy/perf/`(与 `deploy/lab/` 平级,**不改 lab**),docker compose 单栈:

```
┌─────────────────────── docker network: rrperf ───────────────────────┐
│  kv-1  kv-2  kv-3   (kv-store, rocks/strict, TLS+mTLS+token)         │
│  cpuset 各 2 核独占,mem limit 2Gi(对齐 Helm limits)                  │
│  named volume 各一(/var/lib/rr/data)                                 │
│                                                                      │
│  bench-1 (bench-N 可扩)  — rr-perf-bench 负载容器,cpuset 独占 2~4 核 │
│  redpanda (W6 用,profile=kafka)   toxiproxy (W10 用,profile=wan)     │
│  prometheus + grafana(deploy/observability 配置复用)                 │
└──────────────────────────────────────────────────────────────────────┘
```

要点:

- **被测服务 = `kv-store`(真实 KV 语义),不是 EchoSM**。生产用户跑的是自己的 Service;
  kv-store 是仓内最接近的参照(幂等 adapter、read_handle、Kafka ingestor 全真路径)。
- **资源隔离**:每节点 `cpuset` 独占 2 核 + `mem_limit` 2Gi(对齐 Helm `resources.limits`);
  负载容器独立 cpuset。宿主机不足 8 核时降档并在报告 meta 记录实际配额——
  **数值只在同配额矩阵内可比**,报告头部强制打印硬件/配额上下文。
- **网内负载**(R-8 实测教训):客户端 redirect 广告容器 hostname,负载容器必须在
  compose 网络内运行;宿主机直跑会拿不到路由。
- **编排复用 soak.sh 模式**:healthcheck(`/ready`)门控启动顺序;报告落
  `docs/perf/prod/<date>-<sha>/`(raw JSON + prom 快照 + report.md)。

### 3.2 安全配置(过 G1–G7 门)

- 证书:`scripts/gen-dev-certs.sh` 按 perf-lab hostname(如 `kv-1/2/3` SAN)重新生成到
  `deploy/perf/certs/`(**不**复用 `dev-certs/` 的 `rr-N` SAN 证书——SAN 不匹配会握手失败);
  CA 同时挂给负载容器(mTLS 客户端证书 + 服务端验证)。
- token:`RR_ADMIN_TOKEN` / `RR_CLIENT_TOKEN` 经 compose env 注入(与 K8s Secret 同语义,
  不进 git;样例值写在 `.env.example`,真值本地 `.env`,gitignore)。
- `RR_ALLOW_INSECURE` **不存在于该栈任何 env**;stack 自检脚本断言:明文客户端
  (无 TLS)连接必须失败——证明安全门真的开着(负面对照,与 R-8 oracle 负面对照同思路)。
- kv-store 需补 TLS/token 参数面(当前 example 无 TLS flags——见 §6 工作项 M1-a)。

### 3.3 Tier-1(可选升级形态):真实 K8s 集群

Helm chart(`deploy/helm/rr`)即生产制品:3 副本 StatefulSet + PDB + anti-affinity +
TLS Secret + admin token。当具备真实多节点 K8s(非 kind)时:

- `helm install` 生产形态(rocks/strict、TLS、mTLS、`security.allowInsecure=false`);
- 负载从集群**外部**独立机器经 `--cluster-addrs` 打入(NodePort/LB);
- 本方案的工具链(rr-perf-bench 外部模式 + 采集层)拓扑无关,零改动复用。
- **本机无此环境,Tier-1 列为条件触发型里程碑(M5),不阻塞 Tier-0 落地。**

### 3.4 明确非目标(本轮不做)

- 多 group / 多 partition 扩展性(kv-store 与所有 example 均为单 partition → 单 group;
  多 group 需要应用层改造,单独立项);
- 跨地域真 WAN(Toxiproxy 仅提供单机网络造型,W10 为可选情景);
- 故障下性能(R-3/R-4/R-8 已覆盖可靠性维度;本方案聚焦**无故障稳态包线**,
  仅 W9 soak 带 leader 切换作为稳定性情景);
- crates.io 发布、GUI。

## 4. 负载模型(workloads)

统一约定:每个 workload 输出 = 客户端 HDR(按 op 类型分桶)+ TPS + 错误率 +
1s 时间序列分桶 + 服务端 Prometheus 对齐窗口快照。**open-loop 为默认**
(恒定速率 / Poisson 到达,计划时刻调度);closed-loop 仅作对照档保留。

| ID | 负载 | 模型 | 回答的问题 |
|---|---|---|---|
| W1 | 写速率阶梯(step-up:每档 30s,速率递增至明显饱和) | open-loop,uniform key,payload 128B/1KiB/4KiB | 饱和点(knee)= p99 斜率拐点;各目标 TPS 下 p50/p99/p999 |
| W2 | 混合读写 90/10 与 50/50(读 = local read) | open-loop 定速 | 读流量对写提交延迟的挤压 |
| W3 | 线性一致读比例档(0/10/50% lin-read,余 local) | open-loop 定速 | ReadIndex 路径成本与对写路径的影响 |
| W4 | 热点 key:zipfian(θ=0.99)vs uniform,同速率 | open-loop | 单 key 串行化 + 幂等缓存命中率对尾延迟的影响 |
| W5 | payload 扫描 128B/1KiB/4KiB/16KiB | open-loop 定速 | 编解码/复制带宽曲线 |
| W6 | Kafka 摄入:rr-lab-producer 定率生产 → `rr_ingestor_commands_committed_total` 提交速率 + 端到端滞后 | 定率,profile=kafka | 生产主入口链路的稳态速率与 lag |
| W7 | 快照压力:`--snapshot-logs-since-last` 调低(如 5k),W1 同速率重跑 | open-loop | 快照对 p99/p999 的毛刺幅度(时间序列可见) |
| W8 | 安全成本 A/B:同拓扑同负载,TLS+mTLS+token vs(仅本档)insecure 对照栈 | open-loop 定速 | G2 的量化答案:安全层吞吐/延迟税 |
| W9 | 生产配置长 soak(默认 2h,速率 = W1 knee 的 50%)+ 周期性 lin-read 探针;期间一次 `TransferLeader` | open-loop 定速 | steady-state 稳定性、泄漏/退化、切换毛刺 |
| W10(可选) | Toxiproxy 节点间 RTT 造型(1ms/5ms/10ms) | profile=wan | 跨 AZ 近似:RTT 对提交延迟的线性贡献 |
| W11 | **raft log 形态对照(G8/A-0010)**:同 W1 速率阶梯,mem raft log 形态(M0 接线前的旧形态,经 `--storage rocks` + 不接 rocks_base_path 保留) vs rocks-log 生产形态 | open-loop | raft-log 持久化(IoBatcher + fsync)的吞吐/延迟成本;A-0010 前置判据 2 的直接证据 |

> 注:除 W11 对照档外,**全部 workload 只跑 M0 之后的生产形态**(复制 + durable raft log +
> rocks/strict SM);W11 是唯一保留旧形态的基准对照,用于量化修复本身的成本。

## 5. 工具架构(生产性能测试工具)

### 5.1 总体形态

```
deploy/perf/                     ← 新增:生产拓扑编排(§3)
  docker-compose.yml  certs/  prometheus.yml  .env.example  README.md
tools/rr-perf-bench/             ← 既有引擎,纯加法扩展(§5.2)
scripts/prod-perf.sh             ← 新增:一键矩阵入口(§5.3);不动任何既有脚本
docs/perf/prod/<date>-<sha>/     ← 产出存档:raw-*.json + prom/ + report.md
```

### 5.2 rr-perf-bench 加法扩展(推荐方案 A)

既有 S1/S2/S3/soak 的 flag、输出格式、行为**逐字节保持不变**(R-7 基线可比性 +
`perf-baseline.sh` 零改动继续可用)。新增:

1. **open-loop 调度器**(`--open-loop --rate <rps> [--poisson]`):按"计划发起时刻"
   调度请求,延迟 = 计划时刻 → 完成时刻(协调遗漏修正);到达间隔支持恒定与指数分布
   (seed 化 SplitMix64,与仓内故障注入同族,可复现)。新 scenario `Wladder`(W1 阶梯)
   与通用 `--mix write:90,local:5,lin:5`(W2/W3)。
2. **key 分布**:`--key-space <n>` + `--key-dist uniform|zipfian`(W4);写值带
   key 前缀使热点可观测。
3. **多客户端标识**:`--client-id <n>`(多负载容器扇出时聚合用,W1 大速率档);
   JSON meta 记录。
4. **时间序列输出**:`--timeseries-out <path>`(1s 桶:count/p50/p99/err,JSONL),
   G5 毛刺可见性的数据来源。
5. **TLS/auth 客户端**:经 `RrClientBuilder::with_tls(...).with_token(...)`(仓内已有,
   `rr-client` RrClientBuilder)+ `--tls-ca/--tls-cert/--tls-key/--token-env` flags;
   默认明文路径不变。
6. **Prometheus 对齐采集**:run 起止打 UTC 时间戳;`--prom-url <url>` 时在报告里
   内嵌服务端窗口汇总(request/apply/replication RTT/lag/ingestor committed 的
   rate 与分位,来自 recording rules);不带则跳过(Tier-1 无 Prom 时)。
7. **报告生成**:`--json-out` schema  additive 扩展(新字段 optional,旧字段不动);
   `report.md` 由 `scripts/prod-perf.sh` 从 raw JSON 渲染(模板内嵌,无新依赖)。

**备选方案 B(不推荐)**:新建 `tools/rr-prod-bench` crate、rr-perf-bench 完全不动。
优点是完全零触碰;代价是负载引擎/HDR/soak oracle 三大块复制粘贴,后续双份维护,
违反"复用优先"。仅在"方案 A 的加法评审不通过"时启用。

### 5.3 scripts/prod-perf.sh(一键矩阵)

职责(全部为**新文件**,不调用即不修改既有脚本):

1. 构建镜像(本地 Dockerfile,**全部本地镜像,零外网拉取**——遵守环境约定;
   redpanda/prometheus/grafana 镜像 lab 已在用,本机已有);
2. 起 stack → 等 `/ready` → 跑**安全门负面对照**(明文客户端必须失败);
3. 按矩阵顺序执行 W1→W9(每档独立 json + timeseries),档位参数全部环境变量可覆盖;
4. 收尾抓 Prom 快照 + `docker stats` 摘要,渲染 `report.md`,归档到
   `docs/perf/prod/<date>-<sha>/`;
5. `--smoke` 模式(每档 10s)供开发自验;完整矩阵默认时长 ~40 分钟。

### 5.4 与既有资产的关系(再明确)

- `scripts/perf-baseline.sh` / `scripts/soak.sh` / `deploy/lab/` / CI bench job:**零改动**;
- R-7 基线继续是"开发态回归基线";本方案产出"生产态包线基线",两者在报告里并排引用;
- soak oracle(W9 用)直接复用 `rr-perf-bench --scenario soak` 的 ack-log/oracle 实现
  (加法:仅补 TLS client 通路),不另造。

## 6. 里程碑与工作量

| # | 内容 | 产出 | 估时 |
|---|---|---|---|
| M0 | **durable raft log 生产接线(G8 修复,前置阻塞项)**:① `RaftGroupManager` 的 log storage 与 network factory 正交化——`start_group` 在 `rocks_base_path` 已设时用 `RocksLogStorage` + 已配置 network factory(去掉 `start_group_rocks` 的 `NoopNetworkFactory` 硬编码);② `RrNode::start` + watch loop 共 3 处调用点(`node.rs:334/363/459`)走统一入口;③ rr-server main.rs:`--storage rocks` 时设 `rocks_base_path={data_dir}/raft-log`;④ kv-store 增加 `--storage/--data-dir/--durability` 参数面并同样接线(与 M1-a 合并落地);⑤ 测试:3 节点 rocks-log + 复制双开 e2e(重启后已 ACK 不丢,复用 crash harness 模式)+ rr-raft rocks 130 回归。**爆炸半径**:`start_group` 图分析 9 个直接调用全为 rr-raft 内测试(gitnexus,CRITICAL 级但均为单测);RrNode 3 处调用点经文本核实(field-receiver 调用图未收录)。设计细节落地前补 ADR(耐久契约面,A-0011 族) | 生产二进制双开(复制 + durable log);A-0010 重判前提就绪 | 2 人日 |
| M1 | perf-lab stack:compose(kv-store ×3 rocks/strict + cpuset/mem 限额 + Prom/Grafana)+ 证书生成 + token 注入 + 安全门负面对照自检 + README;**M1-a:kv-store 补 TLS/token CLI 参数面**(接 `RrClientBuilder` 同款 TlsConfig,仓内已有全部构件) | `deploy/perf/` 可用,`docker compose up` 全绿 | 1.5 人日 |
| M2 | rr-perf-bench 加法扩展 §5.2 之 1–5(open-loop 调度、mix、zipfian、client-id、timeseries、TLS client);既有 scenario 回归不变(S2 同 flag 重跑输出同形) | 引擎就绪,单测 + 冒烟 | 2.5 人日 |
| M3 | §5.2 之 6–7 + `scripts/prod-perf.sh` 矩阵编排 + report.md 渲染 | 一键矩阵,存档格式定稿 | 1.5 人日 |
| M4 | 首轮完整矩阵(W1–W9 + W11 对照)实测 + 首轮生产基线归档 + 判据表回填;与 R-7 开发态基线并排分析(生产税量化);**A-0010 前置判据 2 重判并回写 `rr-wal-impl.md`** | `docs/perf/prod/` 首份基线 | 1 人日 |
| M5(条件触发) | Tier-1:真实 K8s 上 Helm 生产形态复测核心档(W1/W2/W8) | Tier-1 对照报告 | 1–2 人日 |

合计核心 ~8.5 人日。依赖:M0 阻塞一切(没有双开形态,生产包线无从谈起);M1-a 与 M0④
合并;M2 不依赖 M0/M1(引擎扩展可在 loopback 集群上先验);M4 依赖 M0–M3。

## 7. 验收标准

0. **M0 双开验收(前置)**:3 节点 rocks-log + gRPC 复制同时开启的 e2e 绿(写 N 条 →
   全集群滚动重启 → 已 ACK 全存活、零 head-gap);`rr_raft_append_entries_rtt_seconds
   {kind=append}` 与 `rr_raft_replication_lag` 指标在 rocks-log 形态下正常产出;
   stock `rr-server --storage rocks` 的 raft log 落盘路径为 `{data_dir}/raft-log/{group_id}`
   (文件实证,非仅配置宣称);rr-raft rocks 130 + workspace 全量测试回归绿。
1. `docker compose -f deploy/perf/docker-compose.yml up -d` 全栈 `/ready` 绿;
   明文(无 TLS)客户端连接被**拒绝**(负面对照实测);`RR_ALLOW_INSECURE` 在栈内
   零出现(grep 验证)。
2. `scripts/prod-perf.sh --smoke` 端到端跑通 W1–W9 + W11 缩时矩阵,产出
   `docs/perf/prod/<date>-<sha>/{raw-*.json,timeseries-*.jsonl,prom/,report.md}`。
3. open-loop 与 closed-loop 同速率对照出现在报告中,协调遗漏修正可复算
   (scheduled-vs-actual 字段齐)。
4. 既有资产零改动:`git diff v0.2.0 -- scripts/ deploy/lab/ .github/` 为空;
   `rr-perf-bench` 既有 S1/S2/S3/soak flag 与文本/JSON 输出格式回归一致
   (同 flag 冒烟对比 v0.2.0 二进制输出)。
5. 报告含完整 meta(git SHA/rustc/OS/硬件/配额/安全配置/拓扑),数字可从 raw 数据
   复算;判据表(knee TPS、各负载档 p99)以首轮实测回填,不允许手填。
6. W8 给出量化结论:TLS+mTLS+token 的吞吐/延迟税(同机同配额 A/B)。
7. W11 给出量化结论:raft log 持久化成本(rocks-log vs mem-log A/B);
   **A-0010 前置判据 2 以此数据重判**,结论连同证据回写 `rr-wal-impl.md`。

## 8. 风险登记

| 风险 | 缓解 |
|---|---|
| 单 docker 宿主机仍是单机:绝对数字带宿主烙印 | cpuset 隔离 + meta 强制记录硬件/配额;官方基线注明"同矩阵可比";Tier-1(M5)提供多机真值 |
| macOS Docker Desktop 网络/文件系统失真(virtiofs、gVisor 类开销) | 官方基线优先在 Linux 宿主采集;macOS 数字标注仅供参考;报告 meta 记录 OS |
| 证书 SAN 与 lab hostname 不匹配导致握手失败 | gen-dev-certs.sh 参数化重生成;M1 自检脚本含 TLS 正/负双对照 |
| kv-store 单 partition 限制,无法测多 group | §3.4 明确非目标;若后续立项,kv-store 需支持多 partition 注册 |
| 引擎加法引入既有 scenario 行为漂移 | 验收 #4 的同 flag 回归对比;改动评审以"纯加法"为硬性标准 |
| 矩阵全跑 ~40min 太长,开发不愿跑 | `--smoke` 缩时模式;完整矩阵挂 nightly(信息性,与 soak-nightly 同策略) |
| Prom 快照窗口与客户端窗口错位 | 统一 UTC 时间戳;采集脚本先打点后执行,收尾按窗口查询 |

## 9. 判据模板(首轮回填)

| 指标 | 定义 | 目标 |
|---|---|---|
| knee TPS(写) | W1 中 p99 超过前档 2× 前的最大速率档 | 首轮实测 |
| p99 @ 50% knee | 半饱和点写延迟 | 首轮实测 |
| 安全税 | W8:TLS 档 TPS/insecure 档 TPS;p99 差 | 首轮实测 |
| **raft-log 持久化成本** | W11:rocks-log 档 vs mem-log 档的 TPS/p99 差;fsync 路径(IoBatcher flush 间隔、append RTT kind=append)是否转热 | 首轮实测;**若 rocks-log 显著劣化且 fsync 转热 → A-0010(WAL)重判为具备开工条件** |
| lin-read 成本 | W3:50% lin-read 档读 p99 vs 纯 local 档 | 首轮实测 |
| 快照毛刺 | W7 vs W1:时间序列 p99 峰值倍数 | 首轮实测 |
| soak 稳定性 | W9:首半小时 vs 末半小时 TPS/p99 漂移 <10% | 判据 |
| Kafka 摄入 | W6:稳态提交速率与端到端 lag p99 | 首轮实测 |

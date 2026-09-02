# rr 项目设计与实现全量 Review(2026-08-16)

> **证据基础**:6 个 explore agent 逐层源码审查（全部带 文件:行号）+ 2 轮 Oracle 架构评审 + `cargo check --workspace` 通过（仅测试代码 warning)+ clippy 抽查（rr-storage/rr-config 通过）。
> **范围**:11 个 crate 全部、tests/、fuzz/、benches/、tools/rr-perf-bench、examples/、sdks/java、deploy/(Docker/compose/K8s/Helm/observability)、docs/ 文档站、README。
> **基线**:HEAD=`ba69cc8`(linearizable reads + per-caller rate limiting + WAL ADR);工作树基本干净（`M AGENTS.md`、`M CLAUDE.md` 两处 docs 修改）;GitNexus index 落后 14 个 commit。
> **方法局限声明**:性能维度只审了 bench 基础设施，未跑 benchmark 数字分析（out of scope);与上一轮 review 的 delta 对比未做。

---

## 一、总评

**设计治理远超平均水平，但距 0.2.0 发布有真实阻塞项，其中多个是正确性/durability/可运行性类，不是流程问题。**

| 维度 | 评分 | 一句话 |
|---|---|---|
| 设计 | A- | 10 份 ADR、ADR/plan 分离、semver 承诺、framing 前瞻兼容；错误抽象集中在 GroupMeta.leader 与 manager 膨胀 |
| 实现 | B | 公共库无 anyhow/unsafe/hot-path panic；但存在"每层各自几乎正确、组合后失效"的系统性缝隙 |
| 质量保障 | B- | 测试广度好、CI 矩阵完整；但 rocks 持久化测试 CI 实际跳过、dashboard 引用幽灵 metric——CI 和可观测性在说部分假话 |
| 部署制品 | C+ | quorum 安全设计（OrderedReady/PDB）正确，但 K8s/Helm/Docker 三个默认路径全部无法运行 |

**元根因（Oracle 判定）**：几乎所有 P0/P1 符合同一 pattern——"不变量 X 存在，但 layer Y 没强制"。安全 gate 只管 gRPC、durability 靠隐式 drop-drain、audit 各 feature 自己接、exactly-once 链条某环静默回退、部署制品与 5-gate 安全校验脱节。**功能在涨，横切不变量没有执法者。** 修复方向是写 `invariants.md`：每条系统级不变量标注强制点+对应测试。

---

## 二、设计层面评价

### 做得好的

1. **ADR 治理成熟**:10 份 ADR(9 implemented / 1 accepted),A-0002 对 openraft 契约做了源码级核对，A-0010 WAL 附 precondition(release baseline 先行）而非盲目开工。
2. **正确的删除式决策**:FramedSnapshot/FramedEntry magic+version、`DurabilityPolicy` rename+deprecate、读写 RPC 分离替代 read_only flag(A-0009)、linearizable reads 独立 RPC——都是教科书式的兼容演进。
3. **crate 边界基本合理**:rr-ingestor-kafka feature-gate 隔离 librdkafka;rr-config-etcd 独立；双 trait(`CommandIngestor`/`DynCommandIngestor`）解决 object-safety 干净；rr-storage↔rr-raft 依赖方向正确无循环。
4. **安全骨架完整**:auth env fail-closed、gRPC Raft 强制 mTLS、RaftService 结构性不限流、5-gate 启动校验。

### 设计错误（Oracle 确认）

5. **`GroupMeta.leader` 是错误抽象【P0】**——leader 是 raft 运行时易变状态，放进静态 routing table 必然 stale，违反单一事实源。`rr-core/src/types.rs:22-26` 定义无文档；`rr-config/mem.rs:54` 和 `rr-config-etcd/lib.rs:404` 都用 `replicas.first().copied()` 填充；`rr-router/router.rs:63` 当真实 leader 消费。**修复方式是删除字段**,leader 发现交给 client SDK 的 `NotLeader{leader_addr}` redirect 缓存（0.2.0 已加 leader_addr，正好配套）。
6. **RaftGroupManager ~25 个 public 方法**是"manager 吸收一切"的早期症状——0.2.x 内按 Membership/Snapshot/Admin 分组窄 trait。

---

## 三、实现层面分级问题清单

### P0 — 发布阻塞

| # | 问题 | 位置 |
|---|---|---|
| 1 | **版本号与 breaking change 不匹配**:crate 全 0.1.0,CHANGELOG 已含 0.2.0 级 breaking(`NotLeader` 加 `leader_addr`、`RaftNode::new_with_factory` 签名、tonic server trait 新增 `ReadLinearizable`) | 各 crate Cargo.toml:3 |
| 2 | **rocks 集成测试 CI 实际跳过**:`--features rr-storage/rocks` 未传播到 integration crate rocks feature,`test_restart_persistence` 等 3 个持久化测试不跑 | ci.yml:93 / tests/integration |
| 3 | **snapshot 失败静默吞错**:`get_snapshot_builder` 用 `unwrap_or_default()`,空快照可能被传播 | sm_adapter.rs:92-101 |
| 4 | **TCP raft transport 在安全 gate 之外**:`--raft-transport tcp` 可跑明文 Raft,5-gate 只查 gRPC | node.rs:48-68 vs main.rs:210-232 |
| 5 | **IoBatcher/rocks callback 两处 untracked spawn + stop_group 不 flush**:shutdown durability 靠隐式 drop-drain，不可证明 | io_batcher.rs:116, rocks_log_storage.rs:243 |
| 6 | **`kafka_offset_to_request_id` 非法 bytes 静默回 0** → request_id 回退触发 IdempotencyExpired,exactly-once 承诺被击穿 | rr-ingestor-kafka/lib.rs:74-78 |
| 7 | **`RrError` 无 `#[non_exhaustive]`**（发布后再加本身是 breaking);**change-membership 全链路无 quorum guard**——不只 CLI,AdminService RPC 本身也只查 empty(admin.rs:552/568),quorum-loss check 仅存在于 decommission.rs:174 的 client-side workflow(`MIN_VOTERS_AFTER_REMOVAL=2`)。附注：CONTEXT.md Wave 8 记载的"服务端 quorum-loss check"与代码不符 | error.rs:4, rr-admin/main.rs:675-708, admin.rs:552-568 |
| 8 | **GroupMeta.leader 错误抽象**（见设计 #5) | types.rs:22-26, mem.rs:54, etcd lib.rs:404 |
| 9 | **K8s StatefulSet 默认无法启动**：无 TLS/mTLS/admin_token 且无 allow_insecure,`validate_security_invariants` 直接拒绝 → CrashLoopBackOff | deploy/k8s/statefulset.yaml:143-184 |
| 10 | **Helm 默认无法安装**:`tls.enabled=false` 且无 allow_insecure 开关 | deploy/helm/rr/values.yaml:30-33 |
| 11 | **Dockerfile 构建失败**:builder `rust:1.82` 低于 workspace MSRV `rust-version = 1.88` | Dockerfile:1 + Cargo.toml:25 |

### P1 — 应修

**raft 核心**:
- 每次 append spawn 临时 task 等 batcher，热路径双高开销——append 应直接返回 receiver 让 caller await
- `get_log_state` 全 CF O(n) 扫描；truncate/purge scan+逐条 delete(rocks_log_storage.rs:170-200)
- RocksLogStorage append 的 callback untracked spawn(rocks_log_storage.rs:243)

**network/安全**:
- audit 配对不完整：TriggerSnapshot 无 started、DrainNode 无 finished、reset_ingestor_offset 无 started;ClientService mutation 完全无 audit(admin.rs:228/364-478/602-647)
- `constant_time_eq` 手写+长度早返回，应换 `subtle`(auth.rs:202-211)
- traceparent 文档声称注入 span 但代码未实现，无 interceptor 接线（tracing.rs)
- dashboard 引用不存在的 `rr_auth_rejects_total`(grafana json:176)

**API/SDK**:
- etcd watch 无重连机制；MemConfigProvider watch 静默丢 lag 事件（mem.rs:130);trait 未定义 watch 是否含初始快照
- `token_fingerprint` 仅 32-bit(auth.rs:173-181)

**rr-storage(A-0006 落地偏差)**:
- block cache 未共享：`build_cf_options` 每个 CF 新建独立 `Cache`，与 ADR"shared LRU cache"承诺不符，内存随 CF 数倍增（rocks.rs:253-287);rustdoc 仍称 shared(rocks.rs:156-157)
- `metadata` 及自定义 CF 走 `Options::default()`,tuning 不生效（rocks.rs:359-369, 557-567)
- `MemStorage::write_batch` 遇无效 CF 提前返回但不回滚已应用修改，与 RocksDB WriteBatch 全有/全无语义不一致（mem.rs:86-103)
- A-0006 新增 `raft_log_cf`/`state_machine_cf` 字段未进 config-reference.md / storage-engines.md 文档

**测试/CI**:
- 集成测试 38 处 sleep + 固定端口（51000+node_id@lib.rs:175、19204@lib.rs:1023),flaky 风险
- fuzz 未接 CI,4 个 target 仅 decode 层且无 corpus
- CI "chaos" job 名不副实（实际跑 Kafka/etcd 外部 broker 测试）

**部署**:
- Dockerfile 缺 HEALTHCHECK(Dockerfile:38-51);docker-compose 无健康检查
- K8s/Helm 缺 pod anti-affinity，单节点故障可同时带走 2 pod 破坏 quorum(statefulset.yaml:96)

**docs 系统性漂移**:
- README.md:173 残留旧 4 参数 `RrNode::new` 签名；docs/docs/architecture/c4/components-server.md:80、docs/docs/guide/storage-engines.md:61 同样残留
- docs/docs/guide/getting-started.md:19 写 Rust 1.75+，实际 MSRV 1.88;docs/docs/reference/compatibility.md:119 写 MSRV 1.82，同样不符
- docs/docs/guide/idempotency.md:68 `RrClient::connect(&["node1:50051"])` 类型错误（实际 `Vec<String>`)
- docs/docs/reference/metrics-reference.md:`node.started` 来源写成已删除的 `serve()`(:211);`rr_snapshot_installs_total` group_id 已知问题已修复但文档未更新（:176-178);"Planned Metrics" 仍列已实现的 `rr_raft_replication_lag`(:366)

### P2 — 建议（0.2.1 backlog)

- /metrics /ready 明文无 auth（至少绑 loopback+文档警示）
- rr-client:blocking 接口 current_thread runtime panic 无前置检查、reactive 无背压、退避无 jitter、leader cache 无 TTL
- bench 无回归门槛；rocks benches 因 required-features 未跑；rr-perf-bench Drop 不 wait 且固定端口 47000
- idempotency.rs:96 constructor unwrap;`capture` 序列化整个 LRU 进 snapshot
- bincode 未精确锁定，缺 framed 双向兼容测试
- rr-storage:`JoinError`/RocksDB 错误压平成 `StorageError::Io` 字符串，丢失错误类别（rocks.rs:450-566);`group_commit_interval_ms` 与 Strict 同配时被静默忽略
- 文档以 0.3.0 为当前版本叙述（migration/compatibility),crates/Chart 实为 0.1.0，未披露矛盾
- ADR frontmatter 与索引漂移：A-0001/2/3/4 文件 frontmatter 仍为 `proposed`,ADR README 索引已标 `implemented`（索引为权威入口，对应实现已验证存在，P3/cosmetic)
- Helm 有 serviceAccount 无 RBAC 资源（rr-server 不访问 K8s API，无实际风险，但名不副实）
- Java SDK Docker 集成测试 CI 中被 env gate 跳过；otel-client 是占位示例

### 部署制品中做对的（明确记录)

- `podManagementPolicy: OrderedReady` 与 ordinal-based initContainer 配置渲染匹配，顺序启动保证 bootstrap 身份确定；`Parallel` 会导致 peer 发现不可靠——当前选择正确
- PDB `minAvailable: 2` 等价 maxUnavailable=1，对 3 节点 quorum 安全
- headless Service + PVC ReadWriteOnce 提供稳定网络身份与持久数据身份
- probe 路径与 health.rs 的 /ready /health 一致；Dockerfile 非 root(uid 10001)+ exec entrypoint,SIGTERM 可被 binary 自身处理
- gen-dev-certs.sh 明确 dev-only，无生产路径引用

---

## 四、Oracle 架构判断（5 问）

1. **Shutdown/durability 契约：目前"侥幸安全"不成立**。ACK 边界（fsync 后才 ACK）没被违反，但三个隐式行为叠加使契约不可证明。修复：`IoBatcher` 持有 JoinHandle + 显式 `close()`(drain→final fsync→应答 pending→退出）,`stop_group` await 完成才返回，加"N 条 pending + stop + restart"测试。
2. **TCP transport：设计疏忽**。gate 泛化为 transport-aware config 级校验：TCP 无 TLS 则要求显式 insecure + 拒绝非 loopback bind。不给 TCP 加 TLS(0.2.1+)。
3. **leader 字段：删除，不是修启发式**。
4. **发布阻塞清单**：即 P0 表；non_exhaustive 进 blocker 纯因 API freeze 时机。其余全部 0.2.1。
5. **框架层过厚？** 11 crate 尚属合理，不建议合并。真正要做：`invariants.md` + 构建期跨子系统校验收口 + 一次依赖审计（rr-raft 核心不得出现 Kafka/offset 概念，#6 暗示 offset→request_id 映射可能住错层）+ 每条 advertised 端到端属性（exactly-once/linearizable/durable append）画 invariant chain 图。

---

## 五、建议行动顺序

1. **CI rocks feature 修复**（让 CI 说真话，否则其余修复无法验证）
2. **部署三件套**:Dockerfile Rust 版本、K8s/Helm 默认安全路径（allow_insecure dev 开关或默认要求 TLS secret)
3. **版本对齐 0.2.0** + `cargo semver-checks`
4. **正确性三连**:snapshot 错误传播 → request_id 返回 Result → IoBatcher close/flush
5. **TCP gate + non_exhaustive/quorum guard + 删 GroupMeta.leader**(API freeze 前必须落地）
6. 发 0.2.0;RC 期间并行：invariants.md、audit 配对、docs 漂移修复、dashboard 幽灵 metric

**做与不做**:A-0010 WAL 计划保持 deferred 是对的；P2 清单不值得插队；不合并 crate；不给 TCP 上 TLS(0.2.1+ 再议）。

---

*Review 方法：6 explore（并行后台） + 2 轮 Oracle + L1 验证（cargo check 全 workspace 绿，clippy 抽查绿）。所有 P0/P1 均有 文件：行号 证据，Oracle 抽查 4 项 P0 证据全部属实。纯只读 review，未做任何代码修改。*

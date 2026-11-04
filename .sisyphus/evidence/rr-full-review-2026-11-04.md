# rr 全量无缓存 Review — 2026-11-04

> 触发：用户指令"执行一次全量的无缓存 review，看看我们的目标现在实现到什么程度了；代码质量如何；完成度如何；
> 特别主要要找出来是否有走偏离目标的"。
> 方法：GitNexus 索引强制全量重建（`analyze --force --no-parse-cache`，9,194 节点 / 22,234 边 / 301 簇 / 540 流），
> 7 个并行只读审查 agent（正确性 / 稳定性 / 鲁棒性 / 可观测性 / 易用性 / 性能 / 目标对齐），
> 全部发现要求 file:line 实证。
> HEAD = `1f01cc0`（v0.2.0 + P0 成员安全 + P1 同构建群 + justfile/playbook）。
> 目标锚（docs/docs/intro.md）：G1 分布式正确性与鲁棒性（不可妥协）优先于 G2 高吞吐低延迟（单组 100k ops/s 量级），
> 再优先于 G3 可观测性&易用性。
> **本评审零代码改动**；落地计划见 `.sisyphus/plans/rr-review-2026-11-04-remediation.md`。

## 总评

项目没有跑偏：交付面与目标文档高度对齐，14 份历史 plan 中 11 份有明确闭环/暂缓证据链，P2（副本替换/resharding）
暂缓干净（全仓零半拉子痕迹），kafka/etcd/backup/Java SDK 等大功能全部有目标文档或 ADR 登记，**无未登记的范围蔓延**。
代码基本面保持高水位：vendored openraft 之上无自研共识逻辑、INV 登记执法、TLA+/fuzz/proptest/crash 多层验证网、
错误脱敏带泄漏断言、G1–G7 启动门禁。

当前最大的威胁不是方向错误，而是三件治理层的事：**验证资产停转**（GHA 停用后无本地替代调度）、
**性能目标挂账**（默认配置 ~30–34k TPS，为 G2 目标的 1/3，基线 7 周未重采）、**目标基线未回写**
（formation 子系统超出 intro.md 目标描述但未登记回去）。

用户裁定（2026-11-04）：本项目为个人项目，无外部用户，"破坏性变更在 Unreleased 堆积"不构成风险项，
仅保留发版前版本 bump 的卫生提醒（见计划 Phase F）。

## 维度评分

| 维度 | 评分 | 一句话结论 |
|---|---|---|
| 正确性 G1 | 4/5 | 契约全部登记为可判定不变量且多层咬合；INV-6 路由切换窗口是唯一未闭环缺口 |
| 稳定性 | 4/5 | crash/soak harness 与 oracle 设计 5 分水准，但 nightly 自动化全部停运，判据无人积累 |
| 鲁棒性 | 4/5 | 客户端面加固到位；节点间 RPC 解码无界是与已修复 fuzz OOM 同类的未修缺口 |
| 可观测 G3 | 4/5 | 65 个指标、告警/规则/仪表盘逐一校准；刚交付的 formation 生命周期零观测 |
| 易使用 G3 | 4/5 | 新用户路径逐命令核对无漂移；compose 模板名不副实、Java SDK 与主站指南脱节 |
| 高性能 G2 | 3/5 | 实证文化一流，但回归门禁执行真空、基线陈旧、默认配置仅达目标 1/3 |
| 目标对齐 | 4/5 | 无失控蔓延；扣分在 G2 未达标、执法休眠、plan 文件状态过期、v0.3.0 无范围基线 |

## 验证基线（本会话实测）

- main 全量 `cargo test`：exit 0（含 doc-tests）。
- `scripts/check-invariants.sh`：PASS，9 个 INV、130 条 registry↔test 链接。
- Helm 渲染测试 15/15；真实 etcd 验收（Colima + testcontainers）3/3 + 4/4；Java SDK 单测+集成测试通过。
- 三节点 kv 集群与同构 Genesis 集群（just recipe）真实拉起、选主、复制、拆除全部验证。

## 分维度发现（证据摘要）

### 正确性 G1 — 4/5

- Raft 核心全部走 vendored openraft `0.10.0-alpha.34`（workspace 强制版本，Cargo.toml:38），携带 5 个本地补丁；
  `group_manager.rs`（2674 行）为纯编排层，无自研选举/复制逻辑。自研策略层（成员守卫、learner 提升、formation）
  均有 TLA+ 对应（MembershipGuard TLC 11,545 态零反例）。
- P0 成员安全闭环：quorum guard（rr-core/src/membership.rs:19，`MIN_VOTERS_AFTER_REMOVAL=2`）、
  Forming 拒绝 Admin mutation（admin.rs:127-156 `require_active_group`）、e2e 测试齐（membership_guard.rs）。
- 线性一致读：ReadIndex + ForwardToLeader 重定向（group_manager.rs:1214-1246），无绕过路径；
  porcupine 检查器 + 分区混沌 e2e（tests/integration/src/linearizability.rs）。
- 幂等 apply：namespace cap 1024 fail-loud、typed ApplyOutcome（规避 log-poison，ADR A-0018）、TLA+ OffsetExactlyOnce。
- 快照完整性：tmp→fsync→rename→fsync 目录（snapshot_store.rs:57-122）；C1–C7 fsync-before-ACK 有代码证据。
- 验证网：TLA+ 4 份、fuzz 8 个 target（全为编解码层）、proptest 回归目录已配置但**无种子沉淀**、
  tests/integration ~43 + 16 场景文件、tests/crash A/B/C 三场景。
- **缺口**：① INV-6 路由切换窗口无 fence/epoch（invariants.md:724-729 登记 open，测试靠固定 300ms sleep，
  自认 flaky 模式）；② vendored openraft alpha + 5 补丁是最大单点风险，上游回流/升级流程未文档化；
  ③ fuzz 仅覆盖编解码层，协议交错面证伪依赖未落地的 R-3 turmoil。

### 稳定性 — 4/5

- crash harness：A=SIGTERM 优雅关停、B=种子化随机 SIGKILL + ack-log 回放 oracle（不同 probe payload 防 vacuous）、
  C=快照周期 SIGKILL + 追平校验；zero-panic teardown gate 扫全部 boot 日志；全部 rocks feature 门控。
- 种子化故障注入三处一脉：RR_FAULT_SEED / CRASH_SEED / SOAK_SEED + JSONL fault log 回放。
- soak.sh：默认 2h，四重 oracle（可用性 SLO、ack 回放、panic 计数、lag 收敛）；24h release gating 目标。
- flake 史处理扎实（选举 lease 门控 eb1119e 15/15 轮、INV-1 W9 全链路闭环、force_audit 配置收敛等待）。
- **缺口**：① 8 个 GHA workflow 全部 disabled，crash/soak/fuzz/formal nightly 与 20× 零 flake 判据零自动化积累
  （CONTEXT.md:760 自认待 nightly 累积）；② ENOSPC 无集群级自动化、无关联故障/时钟偏移注入、turmoil 未落地；
  ③ 关停路径 drain 超时降级 warn（group_manager.rs:741）、无后台任务泄漏自动化检测。

### 鲁棒性 — 4/5

- 错误分类学 8 顶层变体 + 7 域枚举全 `#[non_exhaustive]`；脱敏有成文规范 + 防泄露测试
  （不泄露 peer 地址/路径/bincode 内部）。
- bounded_decode 分配前限长（MAX_ENTRY_BODY=16MiB、快照 1GiB），TCP 帧 256MiB cap 先验后读，批次帧 checked_add。
- G1–G7 启动门：G7（TCP 仅 loopback）无条件硬门；G1–G6 可被 allow_insecure 跳过但聚合报错 + 醒告警 + gauge。
- TLS/mTLS、常数时间 token 比较、per-caller 限流（10k caller 上限防洪水）、审计 started/succeeded/failed 成对。
- 管理面：decommission 无 force 通道、change_membership 空 voter 集无条件拒绝、backup tar-slip/SHA-256/原子 rename。
- **缺口**：① 节点间 RPC bincode 解码无界 7 处（lookup.rs:47/86/132、network_factory.rs:541/590/691、
  tcp_transport.rs:492/519）——与 fuzz 修过的 FramedEntry OOM 同类，TCP dev 模式下本地任意进程可 abort 节点；
  ② admin 面错误脱敏不一致（admin.rs:64/994/1098 回显原始内部错误；error_convert.rs:146 ConfigError 透传路径）；
  ③ `grpc_max_frame_size ≤ MAX_ENTRY_BODY` 仅二进制路径强制（config_file.rs:470），库嵌入者 fail-open。

### 可观测性 G3 — 4/5

- 65 个指标全量核对：文档覆盖 63 个；延迟预算链（client→dispatch→proposal→apply）与 replication RTT、
  ingestor 计数器、leader-only lag gauge 全部落地且与 metrics-reference 一致。
- /health /ready /metrics 自实现 hyper 端点，20+ 单测 + 真实 HTTP e2e；告警规则/录制规则/Grafana 面板与代码逐一校准。
- **缺口**：① formation.rs 全文件零日志零指标，也无 Admin RPC/CLI 自省入口——Forming 中集群对运维是黑盒；
  ② 快照传输无字节/耗时指标（`rr_snapshot_installs_total` 只事后计数）；
  ③ 成员变更/leader 转移/手动快照有审计无指标；Vote RPC 无指标；
  ④ trace 关联 DEBUG-only（tracing.rs:98-106），生产 INFO 日志无 trace ID；
  ⑤ 文档漂移：metrics-reference 漏记 2 个指标（rr_deferred_pending_via_non_deferred_total、
  rr_idempotency_namespace_rejects_total）+ 1 处 outcome 标签值；deploy/observability/README 阈值过期
  （45k vs 实际 8000；0.5 vs 1 elections/min）。

### 易使用 G3 — 4/5

- 新用户路径（README → just doctor → kv-cluster-up/genesis-up → playbook）逐命令核对无漂移；
  getting-started 的 rr-server 参数与 clap 定义逐项匹配；启动安全门与写拒绝错误信息带修复路径（可行动性标杆）。
- 5 个 examples 全部可无外部依赖运行（etcd 示例除外且如实标注）；rr-admin JSON 输出 + REPL leader 发现有测试。
- Helm 8 模板 + 6 项静态契约 + 9 项渲染断言 + 防呆占位符（禁 0.2.0 镜像）。
- **缺口**：① deploy/compose/ 只有配置没有 docker-compose.yml，而 CHANGELOG.md:21、intro.md:59、
  node1.toml:11 注释都声称它存在——新用户照文档走 compose 路径立即踩空；
  ② docs/docs/guide/client-sdk.md 全文未提 Java SDK；Java 缺 batching/channel-fanout（Rust 独有）；
  ③ 连接类错误只报症状不给处方（main.rs:87 超时、:1524 discovery 失败、:1564 重连失败）；
  ④ examples 仅 counter 有 README；⑤ sdks/java 4 个 gradle wrapper 文件 dirty 未提交。

### 高性能 G2 — 3/5

- 实测：默认配置单组饱和 ~30–34k TPS（v0.2.0 基线 + 09-22 复测 30.1k）；opt-in 批处理+fanout=4 峰值 121.2k TPS
  @p99 3.0ms（"条件达成"标注诚实）；生产形态（Docker 2 核 + durable + mTLS）128B 膝点 500 ops/s。
- 单 h2 串行点已修：`with_channel_fanout`（rr-client/src/lib.rs:216）+ server 侧流控旋钮，实测 fan8=2.40×；
  但扇出后多组复测未做（roadmap-100k.md:112-115 未勾选）；intro.md:50 归因落后于 roadmap 修正结论。
- IoBatcher 批量落盘 + 64MiB 字节背压、gRPC 连接池、读写 RPC 方法级分离均有实现+测试。
- criterion 8 个 target；rr-perf-bench 场景覆盖良好（closed/open-loop、防协调遗漏、种子复现）。
- **缺口**：① bench-compare.sh 硬门禁（p<0.05 且 >15%）唯一调用点是已停用的 CI；perf-baseline.sh 只采不比；
  prod-perf.sh 只查活不比数；justfile 无 bench/perf/soak 条目——回归发现能力事实停摆；
  ② 基线 7 周未重采，37 个 commit（含 raft 核心路径改动）在零性能门禁下合入；
  ③ dispatch/apply、ShardRouter::route、batch 拆包、ReadIndex、snapshot 均无微基准保护。

### 目标对齐 — 4/5

- 14 份 plan 盘点：11 份有闭环/暂缓证据链（wal-impl 暂缓带量化判据与解锁路径，为教科书式暂缓）；
  rr-quality-hardening 与 rr-prod-perf 的**状态表过期**（已完成但文件未回写）——plan 文件不再是可信状态源，
  实际状态以 CONTEXT.md + CHANGELOG 为准。
- 反向漂移（文档声称但缺失）：crates.io 发布放弃（已诚实改口）、systemd 制品静默消失、
  "跨语言 SDK"复数措辞实际仅 Java。
- P2（副本替换/resharding）：crates/ 全仓 grep 零命中，playbook 与 CHANGELOG 显式声明边界——暂缓干净。
- **formation 子系统（INV-9，formation.rs 1272 行 + etcd lifecycle CAS）超出 intro.md 目标描述未回写登记**——
  方向与 G1/G3 一致、治理良好（有不变量编号 + 13 集成测试 + TLA 边界声明），属"有治理的扩张"。

## 跨维度 Top 风险（按严重度）

1. **验证/回归执行真空**：不变量 lint、bench 门禁、crash/fuzz/soak nightly 全部无执行载体（GHA 已停，本地无调度）。
2. **vendored openraft alpha + 5 本地补丁**：patch #3/#4/#5 直接承载 INV-1；升级/回流流程未文档化。
3. **节点间 RPC 解码无界 7 处**：同 fuzz 已修 bug 类，修法现成（with_limit）未应用。
4. **formation 生命周期零观测零自省**：P1 旗舰特性是运维黑盒。
5. **INV-6 路由切换窗口 open**：唯一未闭环的正确性契约。
6. **G2 决策点悬置**：ordered 旋钮、多组扇出复测、100k 目标立场均无裁定。

## 用户裁定记录

- 2026-11-04：项目为个人项目、无外部用户——"破坏性变更在 Unreleased 堆积"不构成风险项，降级为发版前版本卫生
  提醒（版本 bump + 迁移注记），不再列为治理风险。

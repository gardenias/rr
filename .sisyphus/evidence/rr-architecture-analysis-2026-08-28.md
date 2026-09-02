# rr 项目架构全景分析(2026-08-28 归档)

> **性质**:架构认知归档(非代码审查)。基于 2026-08-27 对全仓的系统性走读,结论归档供后续工作引用。
> **基线**:HEAD=`ba69cc8`(linearizable reads + per-caller rate limiting + WAL ADR);GitNexus 索引与该 commit 同步(4235 symbols / 10058 edges / 173 communities / 300 processes)。
> **输入**:README、CHANGELOG、release-notes/v0.2.0、docs/docs/architecture/*(crate-map + C4 13 图)、`.sisyphus/adr/`(A-0001~A-0010)、`.sisyphus/plans/rr-wal-impl.md`、`.sisyphus/evidence/rr-full-review-2026-08-16.md`(下称"8-16 review")、全部 crate 源码结构。
> **与 8-16 review 的关系**:本归档不改写其任何结论;8-16 review 的 P0/P1 清单仍是发布门禁的权威依据(HEAD 与其基线相同,行号引用仍有效)。

---

## 一、定位

rr 是 Rust 编写的**分布式 Multi-Raft 共识框架**:单节点托管多个独立 Raft 组(openraft 0.10.0-alpha.19 底座),用**路由表分片**(非哈希环)做数据分布。上层业务实现 `Service`/`StateMachine` 两个 trait 即可获得强一致复制能力。配套:gRPC 网络层、可插拔存储(内存/RocksDB)、ConfigProvider(内存/etcd)、客户端 SDK(Rust + Java)、admin CLI、完整部署制品。

## 二、架构分层(无循环依赖)

| 层 | Crate(约行数) | 职责 |
|---|---|---|
| 基础 | rr-core (2.3k) | 全部契约 trait:`Service`、`StateMachine`、`StorageEngine`、`ConfigProvider`、`KafkaProducer`;`RrError` 分类;codec/framing/幂等/ingestor 抽象 |
| 基础设施 | rr-raft (8.3k,最大) | `RaftGroupManager`(多组)、`RaftNode`、内存/RocksDB 两种 log 存储、`IoBatcher` 组提交、快照、`output_runner`(outbox)、`ingestor_runner`、`learner_promoter`、gRPC+TCP 双 transport |
| | rr-network (2.9k) | gRPC 三服务(Raft/Client/Admin)、`ChannelPool`、bearer auth、令牌桶限流(含 per-caller)、TLS、audit、traceparent |
| | rr-storage (1.4k) | `MemStorage`/`RocksStorage`(3 CF + per-CF tuning)、`DurabilityPolicy`(Strict/ReplicatedOnly/TestOnly) |
| | rr-router (0.3k) | `ShardRouter`:partition → group 路由表解析 |
| | rr-config / rr-config-etcd | `MemConfigProvider`(broadcast 通知)+ etcd 实现(watch) |
| 编排 | rr-server (5.3k) | `RrNode` 生命周期、clap CLI、TOML 严格 schema(CLI>env>file>default)、dispatch/lookup、HTTP 健康端点、5-gate 安全校验 |
| 应用 | rr-client (1.2k) | SDK:自动 leader 重定向+缓存、重试、async/blocking/reactive 三种 API |
| 工具 | rr-admin (2.9k) | 状态/快照/transfer-leader/离线备份恢复/decommission/node-reset |

外围:Java SDK(Gradle)、rr-perf-bench(3 进程基准)、4 fuzz target、8 criterion bench、tests/integration(ClusterBuilder + 双向故障注入)。

## 三、关键机制

- **写路径**:ClientRequest → `ClientDispatchImpl` → `ShardRouter` → `RaftGroupManager` → openraft append(IoBatcher 组提交)→ apply → `ServiceStateMachineAdapter` → 用户 `Service.handle`;leader 变更经 `NotLeader { leader, leader_addr }` 由 SDK 重定向
- **线性一致读(A-0009)**:独立 `ReadLinearizable` RPC(协议层强制、不可降级),ReadIndex + 本地 apply;follower 返回 `leader_info` 供 SDK 重拨;读写 RPC 分离
- **Exactly-once 输入(A-0001+A-0003)**:Kafka ingestor 将 offset 映射为 request_id,走框架级幂等层去重
- **Outbox(A-0007)**:leader-only 后台任务 drain per-group outbox,at-least-once 投递(KafkaPublish/RaftWrite),指数退避,4 个新 metric
- **快照(A-0002/A-0008)**:`LogsSinceLast(50_000)` 可配,节点间快照传输
- **安全**:mTLS(Raft 唯一认证方式)、bearer token(fail-closed、常量时间比较)、全局+per-caller 限流(**RaftService 结构性不限流**)、`rr::audit` 审计、启动 5-gate 校验 + `allow_insecure` 显式逃生门、错误信息消毒

## 四、工程治理

- 10 份 ADR(9 implemented / 1 accepted),ADR 与实施 plan 分离;A-0002 对 openraft 契约做过源码级核对
- C4 文档站:13 张 PlantUML C4 图 + 5 份运维 runbook + 中英双语 guide(Docusaurus)
- CI 13 个 job;4 个 chaos 测试经双向故障隔离改造后全部进入默认 CI
- 部署制品:Dockerfile、compose、K8s StatefulSet+PDB(OrderedReady + ordinal initContainer 的 quorum 安全设计正确)、Helm chart、release workflow 推 ghcr

## 五、现状判断(与 8-16 review 一致)

设计治理远超平均(ADR/semver/framing 前瞻兼容),但 **v0.2.0 发布存在 11 个 P0 阻塞项**,且多为正确性/durability/可运行性类:

1. 版本号未对齐 0.2.0(crate 仍 0.1.0,CHANGELOG 已含 breaking)
2. rocks 持久化集成测试 CI 实际跳过(ci.yml:93 feature 未传播)
3. snapshot `unwrap_or_default()` 静默吞错(sm_adapter.rs:92-101)
4. TCP raft transport 在 5-gate 安全校验之外(可跑明文 Raft)
5. IoBatcher/rocks callback 两处 untracked spawn + `stop_group` 不 flush——shutdown durability 不可证明
6. `kafka_offset_to_request_id` 非法 bytes 静默回 0 → exactly-once 被击穿
7. `RrError` 无 `#[non_exhaustive]`;change-membership 全链路无 server-side quorum guard
8. `GroupMeta.leader` 错误抽象(易变运行时状态放入静态路由表)
9-11. K8s/Helm/Docker 三个默认部署路径全部无法运行(Dockerfile rust:1.82 < MSRV 1.88 等)

**元根因(Oracle 判定)**:横切不变量没有执法者——功能在涨,系统级不变量(durability/exactly-once/安全 gate/部署与安全校验的一致性)没有统一的强制点与测试。修复方向:`invariants.md`(每条不变量标注强制点+测试)。

**A-0010 WAL 保持 deferred 是正确的**:待 release-build 基线证明 RocksDB LSM 是瓶颈再开工。

## 六、后续工作索引

- 发布路线图与全部工作项设计:`.sisyphus/plans/rr-v0.2.0-release-roadmap.md`
- 可靠性专项(正确性/性能保障体系):`.sisyphus/plans/rr-reliability-program.md`
- 新增 ADR:A-0011(shutdown durability 契约)/ A-0012(membership quorum guard)/ A-0013(删除 GroupMeta.leader)/ A-0014(TCP transport 安全 gate)
- 实施计划:`.sisyphus/plans/rr-durability-hardening.md`
- 不变量目录:`docs/docs/reference/invariants.md`

---

*归档方法:只读分析(GitNexus 图谱 + 文档 + 源码结构),未做任何代码修改。*

# rr v0.2.0 发布路线图与可靠性专项 — 主计划

> **状态**:**已闭环(2026-09-10 `v0.2.0` final tag 已 cut)**:Phase 0 已实施(2026-08-29:WS-1~WS-7 全部落地、全仓 clippy/测试全绿;ADR A-0011~A-0014 → implemented;见 CONTEXT.md 2026-08-29 session);RC 并行项已全部关闭(2026-09-04,见下文各勾选 + freeze checklist B/D/E 裁决记录);Phase 1 可靠性专项 R-1~R-8 全部关闭(2026-09-05~10,见 rr-reliability-program.md 与 CONTEXT.md 各批次记录)
> **依据**:`.sisyphus/evidence/rr-full-review-2026-08-16.md`(11 个 P0、P1/P2 清单、Oracle 5 问)+ `.sisyphus/evidence/rr-architecture-analysis-2026-08-28.md`(全景归档)
> **基线**:HEAD=`ba69cc8`;8-16 review 的行号引用与当前代码一致。
> **原则**:正确性 > 性能 > 功能;设计与方案先行(本文件 + 各 ADR/plan 落地后才动工);每个工作项带验收标准。

---

## 总体判断

v0.2.0 的阻塞不在功能完整性,而在**横切不变量没有执法者**(Oracle 元根因)。因此本路线图的编排逻辑是:

1. **先让 CI 和部署制品说真话**(WS-1/WS-2)——否则后续所有修复无法被验证;
2. **再修正确性三连与 API freeze**(WS-3/WS-4/WS-5/WS-6/WS-7)——这些是"发布后再改就是 breaking"或"静默丢数据"级问题;
3. **RC 期间并行建可靠性体系**(WS-8 + 可靠性专项 R-1~R-8)——把每条对外宣称的属性(exactly-once、linearizable、durable append)配上不变量链 + 强制点 + 测试。

---

## Phase 0 — v0.2.0 发布阻塞项(P0)

顺序即依赖顺序。WS-1/2/3 为任务级(设计要点在本文件内);WS-4/5/6/7 有独立设计文档。

### WS-1 — CI 真实性修复(0.5~1 人日,最高优先级)

**依据**:review P0#2、P1(测试/CI)、P2。

- [ ] **rocks 持久化测试 CI 实际跳过**:`.github/workflows/ci.yml:93` 的 `--features rr-storage/rocks` 未传播到 `tests/integration` crate,导致 `test_restart_persistence` / `test_node_failure_recovery` / `test_crash_during_write_preserves_acked_entries` 3 个持久化测试从未在 CI 运行。
  - 方案:`tests/integration/Cargo.toml` 增加 `rocks` feature 并透传 `rr-storage/rocks`;CI 改为 `cargo test -p integration-tests --features rocks`;在 CI 日志中确认 3 个测试实际执行(非 skipped)。
- [ ] **CI "chaos" job 名不副实**:实际跑的是 Kafka/etcd 外部 broker 测试。重命名 job(如 `external-broker`),真正的 chaos 测试已在默认 test job,无需重复。
- [ ] **Grafana dashboard 幽灵 metric**:`deploy/observability` 的 grafana json:176 引用不存在的 `rr_auth_rejects_total`。二选一:在 `rr-network::auth` 补该 counter(推荐,审计配对顺带做),或修正 dashboard 引用现存 metric。
- [ ] **rocks benches 因 `required-features` 从未运行**(review P2):修正 benches/Cargo.toml,使 `log_append_rocks` 在 bench job 中执行。

**验收**:CI 日志可见 rocks 持久化测试与 rocks bench 实际运行;dashboard 全部 panel 引用存在的 metric;job 名称与内容一致。

### WS-2 — 部署制品修复(1~2 人日)

**依据**:review P0#9/#10/#11、P1(部署)。设计要点:部署默认路径必须与 `RrNode::new` 的 5-gate 安全校验(`validate_security_invariants`)**自洽**——要么默认安全(TLS+token),要么显式 `allow_insecure`,不允许"默认配置直接 CrashLoopBackOff"。

- [ ] **Dockerfile**:`rust:1.82` → 与 workspace MSRV 一致的 `rust:1.88`(或 `rust:1.88-slim`);补 `HEALTHCHECK`(runtime stage 安装 `curl` 或用 `rr-admin node status` 本地探活,打在 `/ready` 上)。
- [ ] **K8s manifests**(`deploy/k8s/statefulset.yaml:143-184`):默认引用 TLS Secret + `admin_token` 的 env 引用样例;为无 TLS 的本地试玩场景提供**显式** `RR_ALLOW_INSECURE=true` 的 kustomize overlay 或注释块(默认关闭)。补 pod anti-affinity(`statefulset.yaml:96` 缺失,单宿主机故障可同时带走 2 pod 破坏 quorum):`podAntiAffinity` preferredByHostname。
- [ ] **Helm chart**(`deploy/helm/rr/values.yaml:30-33`):新增 `security.allowInsecure`(默认 `false`),`tls.enabled=false 且 allowInsecure=false` 时 `fail` 模板直接拒绝渲染并给出提示;anti-affinity 规则入模板。
- [ ] **docker-compose**:补 healthcheck 段(`/ready`),`depends_on: condition: service_healthy`。

**验收**:`docker build` 成功;`helm template` 默认渲染被拒绝且报错信息可读,`--set security.allowInsecure=true` 可渲染;kind 集群上 3 节点 Ready、探杀 1 pod 后 quorum 保持。

### WS-3 — 版本对齐与 API freeze(0.5 人日 + 审查)

**依据**:review P0#1/#7(部分)、Oracle Q4。

- [ ] **crate 版本对齐 0.2.0**:11 个 crate `Cargo.toml` 的 `version`(当前 0.1.0)与 CHANGELOG 的 0.2.0 级 breaking 不符。方案:`workspace.package.version = "0.2.0"` 统一声明,各 crate `version.workspace = true`;Helm `Chart.yaml` 的 `appVersion`、Java SDK(已 0.2.0,核查 gradle 文件)对齐。
- [ ] **`#[non_exhaustive]` 审计**:`RrError`(`rr-core/src/error.rs:4`)及各分类错误 enum(`RaftError`/`StorageError`/`ConfigError`/`NetworkError`/`CodecError`)逐一评估;对外暴露的 pub enum 在 0.2.0 冻结前加注,发布后增补 variant 才不构成 breaking。
- [ ] **cargo-semver-checks 入 CI**:对 0.1.0 baseline 允许失败(已知 breaking),作为 0.2.0 之后的门禁运行。
- [ ] **API freeze checklist**:全部 `pub` item 过一遍(doc 完整性、该 `pub(crate)` 的收回、`#[doc(hidden)]` 标记内部项);deprecated alias(`DurabilityPolicy::Balanced`/`Fast`)保留策略写入 compatibility.md。

**验收**:`cargo semver-checks` 在 CI 常驻;版本号全仓一致(grep 验证);freeze checklist 签字。

### WS-4 — 正确性三连 + shutdown durability 契约(3~5 人日,最高技术风险)

**依据**:review P0#3/#5/#6、Oracle Q1、P1(raft 核心)。

设计文档:**ADR `A-0011`**(`.sisyphus/adr/A-0011-shutdown-durability-contract.md`)+ 实施计划 `.sisyphus/plans/rr-durability-hardening.md`。

范围概要:
1. snapshot builder `unwrap_or_default()` → 错误传播(`sm_adapter.rs:92-101`);
2. `kafka_offset_to_request_id` 非法 bytes → 返回 `Result`,消灭静默回 0(`rr-ingestor-kafka/lib.rs:74-78`),并审查 offset→request_id 映射是否住错层(Oracle Q5 的依赖审计);
3. `IoBatcher` 显式生命周期:tracked spawn(持 `JoinHandle`)+ `close()` 协议(drain → final fsync → 应答 pending → 退出);`rocks_log_storage.rs:243` callback spawn 同样收口;
4. `stop_group` 必须 await 全部 pending append flush 完成才返回;
5. 顺手修 P1 热路径:append 不再 spawn 临时 task 等 batcher,改为直接返回 receiver 由 caller await(性能+正确性双收)。

**验收**:新增"N 条 pending + stop + restart"测试证明已 ACK 数据不丢;`grep` 全仓无 untracked `tokio::spawn` 于写路径;kill -9 混沌场景在 CI 可重复验证。

### WS-5 — TCP raft transport 安全 gate(0.5~1 人日)

**依据**:review P0#4、Oracle Q2。

设计文档:**ADR `A-0014`**(`.sisyphus/adr/A-0014-tcp-transport-security-gate.md`)。

范围概要:`validate_security_invariants` 泛化为 transport-aware:`--raft-transport tcp` 且无 TLS 时,要求显式 `allow_insecure` **且** bind 地址必须是 loopback,否则拒绝启动;**不给 TCP 加 TLS**(0.2.1+ 再议,避免范围蔓延)。

**验收**:明文 TCP + 非 loopback + 无 allow_insecure 的启动被拒绝,错误信息可读;现有 gRPC gate 行为不变(回归测试)。

### WS-6 — change-membership server-side quorum guard(1~2 人日)

**依据**:review P0#7、P1(docs 漂移中的 CONTEXT.md Wave 8 记载与代码不符)。

设计文档:**ADR `A-0012`**(`.sisyphus/adr/A-0012-membership-quorum-guard.md`)。

范围概要:AdminService 的 change-membership RPC(`admin.rs:552/568` 当前仅查 empty)增加 server-side quorum-loss 防护;`decommission.rs:174` 的 `MIN_VOTERS_AFTER_REMOVAL=2` client-side 逻辑上移为 server-side 共享守卫;提供显式 `force` 逃生参数(审计事件必记录);修正 CONTEXT.md 的不实记载。

**验收**:会导致 quorum loss 的 membership 变更在 server 端被拒绝(集成测试:3 节点集群试图摘除 2 个 voter 被拒);force 路径有 audit 事件。

### WS-7 — 删除 `GroupMeta.leader`(2~3 人日,breaking,必须赶在 API freeze 前)

**依据**:review P0#8、设计#5、Oracle Q3("删除,不是修启发式")。

设计文档:**ADR `A-0013`**(`.sisyphus/adr/A-0013-remove-groupmeta-leader.md`)。

范围概要:删除 `rr-core/src/types.rs:22-26` 的 `leader` 字段;清理 `rr-config/mem.rs:54`、`rr-config-etcd/lib.rs:404` 的填充逻辑、`rr-router/router.rs:63` 的消费;leader 发现统一走 client SDK 的 `NotLeader { leader_addr }` redirect 缓存(0.2.0 已加 `leader_addr`,正好配套);评估对 A-0004 etcd key schema 的 wire 影响并给迁移说明。

**验收**:路由更新相关集成测试全绿;etcd schema 文档同步;SDK 在 leader 切换后无需 `GroupMeta.leader` 即可完成重定向(由 WS-6/现有 chaos 测试覆盖)。

---

## Phase RC — 发布候选期并行项

### WS-8 — invariants.md v1(2~3 人日,元根因修复的第一步)

**依据**:review 元根因 + 建议行动顺序#6。

交付物:`docs/docs/reference/invariants.md` —— 每条系统级不变量(见可靠性专项 R-1 的分类)标注:**不变量陈述 → 强制点(file:line)→ 现有测试 → 已知缺口**。v1 覆盖 8 条 advertised 属性;之后作为 living document,凡新增对外属性必须先登记不变量。

### RC 其余并行项(任务级,1~2 人日合计)

> 全部关闭(2026-09-04)。验证:rr-client 27 测试、rr-server 110 测试、rr-network 71 测试、integration(含新 linearizable read 测试)全绿;workspace clippy `-D warnings` 零告警。

- [x] **audit 配对补齐**(2026-09-04):核实现状——`TriggerSnapshot` 已有 dispatch+result 配对;补 `DrainNode` 主路径 finished 事件(原仅 no-op 分支有 result)与 `reset_ingestor_offset` dispatch 事件。**ClientService 热路径 audit 经评审不做**:audit-log.md 文档面明确 audit 覆盖范围为 AdminService,逐写请求 audit 会冲垮热路径与日志链;该清单项引用的 `admin.rs:228/364-478/602-647` 实为 AdminService 的三个 mutation RPC,已全部配对。
- [x] **docs 漂移清单**(逐条修复):`README.md` 旧签名/`getting-started.md` MSRV/`compatibility.md` MSRV/`idempotency.md` connect 类型——核实均已在先前提交修复;`metrics-reference.md` 三处本次修(`rr_snapshot_installs_total` group_id 已知问题过时、Planned Metrics 列已实现项;`:211` `node.started`→`start()` 早已正确);**0.3.0→0.2.0 版本叙述统一**(idempotency/kafka-input/metrics-reference/audit-log/durability/upgrading + `client.proto` 注释 + `config_file.rs` 注释);`v0.2-to-v0.3-migration.md` 改名重写为 `v0.1-to-v0.2-migration.md`(sidebar/交叉链接同步,docusaurus build 通过)。
- [x] **linearizable read 3 节点集成测试**(A-0009 遗留 follow-up):`test_read_linearizable_redirect_from_follower`(2026-09-04)——SDK 仅指向已知 follower,断言经 leader_info redirect 收敛成功;集成测试 EchoSM 补 `read_handle`(镜像生产 EchoStateMachine)。
- [x] **ADR frontmatter 与索引漂移**(P3/cosmetic):A-0001/2/3/4 frontmatter `proposed` → `implemented`(2026-09-04),与索引对齐。
- [x] **`constant_time_eq` 换 `subtle`**(2026-09-04):手写实现替换为 vetted `subtle::ConstantTimeEq`(workspace 依赖,无新增供应链);`token_fingerprint` 32-bit → 64-bit(16-hex,birthday 碰撞域 ~2^32),audit-log.md 同步。
- [x] **rr-client 静默假成功修复**(2026-09-04,P1):`send_request_inner` 重构——`success=false` 一律按可重试处理:有 `leader_info` 更新缓存,无则清缓存走 bootstrap 轮询,重试耗尽返回 `RaftError::NotLeader`(修掉原两条静默 `Ok(空 payload)` 路径:无 leader_info 分支 + 末次 attempt 的 leader_info 分支)。回归测试 2 个(mock gRPC server:leader 未知窗口不再假成功、leader 出现后收敛)。同批顺带:blocking 接口 current_thread/无 runtime 由 panic 改为前置检查返回错误(freeze checklist E 节 P2)+ 2 测试。影响面(gitnexus impact,CRITICAL 级但行为不变式收敛):redirect/failover 相关 5 个集成测试实测全绿。
- [x] **docs 漂移增补**(同上来源):核实 `dynamic-config-update.md`/`dynamic-leader-election.md`/`dynamic-put-request.md`/`leader-flapping.md` 已无 `update_leader`/`LeaderChanged` 残留(grep 零命中,先前提交已修)。

**RC exit criteria**:Phase 0 全部 WS 验收通过 + 上述并行项关闭 → 才允许 cut `v0.2.0` tag。

---

## Phase 1 — 可靠性专项(post-0.2.0,部分可在 RC 并行启动)

详见 **`.sisyphus/plans/rr-reliability-program.md`**。项目索引:

| ID | 项目 | 一句话 | 建议启动 |
|---|---|---|---|
| R-1 | 不变量目录与强制点执法 | = WS-8 交付物,之后为 living document | RC |
| R-2 | TLA+ 形式化 | 对 rr 扩展协议(membership guard、routing 切换、output watermark、offset→request_id 链)建模;openraft 核心已有自己的验证,不重复 | RC 后 |
| R-3 | 确定性仿真测试 | turmoil 试点:chaos 测试确定性化,失败可回放 | 0.2.1 |
| R-4 | crash-recovery + 存储故障注入 | kill -9 随机点 + 重启 + durability oracle("已 ACK 必存活");fail fsync/torn write/ENOSPC 注入层 | RC 后(与 WS-4 衔接) |
| R-5 | 线性一致检查器 | porcupine 风格 Rust checker,kv-store register 模型,历史采集自 chaos/集成测试 | 0.2.1 |
| R-6 | proptest/fuzz 扩展 | router 变迁、幂等 LRU、batcher 交错、codec 双向兼容矩阵;fuzz corpus 入仓 + CI nightly | 0.2.1 |
| R-7 | 性能基线与回归门禁 | release-build 基线(**顺带完成 A-0010 前置条件**);criterion baseline compare 入 CI | RC |
| R-8 | soak 自动化 | 24h+ compose 集群 + 故障注入循环,release 前必跑 | 0.2.1 |

A-0010 WAL 维持 deferred:R-7 的 release 基线是它的前置门槛,基线出来前不动工。

---

## 依赖与关键路径

```
WS-1 (CI 真话) ──┬──> WS-4 (正确性三连, 需 CI 验证) ──> RC
                 ├──> WS-2 (部署制品, 独立)
WS-3 (版本/API) ──┴──> WS-7 (删 leader, breaking, freeze 前必须完成)
                       WS-5/WS-6 (gate/guard, 相互独立, 依赖 WS-1 的验证能力)
WS-4 ──> R-4 (crash harness 复用 WS-4 的 close 协议)
WS-8 ──> R-1/R-2 (invariants.md 是形式化的输入)
R-7 (release 基线) ──> A-0010 (WAL 开工判据)
```

**关键路径**:WS-1 → WS-4 → RC exit。建议 WS-1/WS-2/WS-3 立即并行开工,WS-4 紧随其后。

## 风险登记

| 风险 | 缓解 |
|---|---|
| WS-4 改 batcher 生命周期引入回归 | 先在 plan 中固化 callback 契约;每步配套测试;WS-1 先保证 CI 能验证 |
| WS-7 删字段影响 etcd schema(A-0004)与存量数据 | ADR A-0013 必须含 wire 兼容与迁移章节;RC 期用真实 etcd 做升级演练 |
| P0 修复挤压 RC 时间,可靠性专项被无限推迟 | WS-8 与 R-7 在 RC 期并行启动;R-3/R-5 明确排进 0.2.1 里程碑而非"有空再做" |
| openraft 停留在 0.10.0-alpha.19,上游 API 漂移 | 版本已精确钉死(`= 0.10.0-alpha.19`);升级单独开 ADR 评估,不混入本路线图 |
| 集成测试 38 处 sleep + 固定端口的 flaky 风险(review P1) | R-3 的 turmoil 确定性化是根治;短期:固定端口改动态分配可随 WS-4 顺带做 |

## 工作量汇总(粗估,1 人全力)

- Phase 0:8~14 人日
- Phase RC:4~6 人日
- Phase 1(R-1~R-8 全量):20~30 人日,跨 0.2.1~0.3.0 两个里程碑

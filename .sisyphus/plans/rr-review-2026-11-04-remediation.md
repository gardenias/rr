# rr Review 修复工作计划 — 2026-11-04

> 来源：`.sisyphus/evidence/rr-full-review-2026-11-04.md`（全量无缓存 review，HEAD=`1f01cc0`）
> 目标：修复审查发现的缺口，恢复执法机器，保持 G1 > G2 > G3 优先序。
> 裁定记录：个人项目无外部用户，破坏性变更堆积不构成风险（2026-11-04 用户裁定），仅保留 F2 版本卫生。

## 状态

| 阶段 | 内容 | 状态 |
|---|---|---|
| A | 执法恢复（本地调度替代已停用的 CI 职能） | ⏳ 待开始 |
| B | 正确性/鲁棒性缺口（G1） | ⏳ 待开始 |
| C | 可观测性欠账（G3-观测） | ⏳ 待开始 |
| D | 性能治理（G2） | ⏳ 待开始 |
| E | 易用性/文档债（G3-易用） | ⏳ 待开始 |
| F | 工程治理（低优先） | ⏳ 待开始 |

---

## Phase A — 执法恢复（一切质量工作的前提）

### A-1: 恢复本地定期执法调度

**问题**：R-1~R-8 的全部强制点（不变量 lint、bench 回归门禁、crash/fuzz/soak nightly）载体是 GitHub Actions，
自 2026-10-20 全部停用后无任何替代执行者；`CONTEXT.md:760` 自认的"20× 零 flake"判据与 24h soak 门禁零积累。
验证资产存在但持续贬值——这是所有其他质量工作的前提。

**方案**：
1. justfile 增加聚合 recipe：`nightly`（check-invariants + crash 套件 + fuzz 短跑 + integration 全量）、
   `bench-gate`（criterion + bench-compare 对比最近一次基线）、`soak-smoke`（soak.sh 短配置）。
2. 提供 `scripts/install-local-gates.sh`：用 launchd plist（或 cron）注册每日/每周定时任务，
   运行结果（含退出码、摘要）落 `target/quality-runs/<date>/` 目录，失败时响亮通知（日志 + 非零退出）。
3. 不依赖 GHA，不恢复任何 workflow。

**验证**：连续 3 天有 `target/quality-runs/` 成功记录；人为引入一个回归（如改动某 INV 测试名）能被
check-invariants 拦下。

### A-2: vendored openraft 补丁台账与升级流程

**问题**：vendored openraft `0.10.0-alpha.34` + 5 个本地补丁，其中 patch #3/#4/#5 直接承载 INV-1（已 ACK 不丢）。
alpha 基座 + 分叉是最大单点风险，上游修复回流与版本升级路径未文档化。

**方案**：在 `vendor/openraft/` 或 `docs/docs/architecture/openraft-relationship.md` 增加补丁台账：
每个补丁的目的、承载的不变量、上游 issue/PR 状态、升级 openraft 时的核对清单（哪些补丁可弃、哪些必须移植）。

**验证**：台账文件存在且 5 个补丁逐条对应 `git log -- vendor/openraft`；文档站可访问。

---

## Phase B — 正确性/鲁棒性缺口（G1）

### B-1: 节点间 RPC 解码套用 bounded_decode

**问题**：节点间 RPC 的 bincode 解码全部无长度上限——`crates/rr-network/src/lookup.rs:47/86/132`、
`network_factory.rs:541/590/691`、`tcp_transport.rs:492/519`，共 7 处；与 fuzz 已修复的 FramedEntry OOM
（9 字节输入→2.5GiB 分配→abort）是同一 bug 类。TCP dev 模式下本地任意进程可 abort 节点；
mTLS 下被攻陷的 peer 亦可。另：快照内嵌幂等状态二次解码无界（`crates/rr-raft/src/idempotency.rs:224`）。

**方案**：全部改用现有 `bounded_decode` / `with_limit`（上限取各消息类型的合理值，
如 AppendEntries ≤ MAX_ENTRY_BODY×批大小）；
为每处补"超限输入被拒且连接存活"的单元测试；把对应 fuzz target 扩展到节点间消息解码。

**验证**：新增测试证明超限输入返回错误而非 abort；`cargo test -p rr-network -p rr-raft` 通过；fuzz 短暂冒烟无发现。

### B-2: 库嵌入路径的配置校验补强

**问题**：`grpc_max_frame_size ≤ MAX_ENTRY_BODY` 的硬耦合只在二进制配置路径强制（`config_file.rs:470-481`），
`NodeConfig::validate`（`config.rs:344-390`）不查——库嵌入者配错会把毒 entry 写入日志，apply 期组致命（ADR A-0018 类）。
G7 loopback 硬门同样可被库用户直接 `TcpRaftServer::bind` 绕过。

**方案**：把 frame-size 耦合校验上移进 `NodeConfig::validate`，二进制与库路径共用；
`TcpRaftServer::bind` 的绕过面在文档中升级为显式 unsafe 约定或加 debug_assert 告警。

**验证**：新增 `NodeConfig::validate` 拒绝错配的单元测试；既有 `config_file.rs` 测试保持绿。

### B-3: Admin 面错误脱敏不一致

**问题**：`crates/rr-server/src/admin.rs:64`（fetch_snapshot_bytes）、`:994`（trigger snapshot）、
`:1098`（get_routing_table）绕开脱敏直接 `format!("...: {e}")` 回显原始内部错误；
`error_convert.rs:146` 的 `ConfigError::InvalidConfig(msg)` 原样透传（msg 可能含 PEM 路径）。
违背自家脱敏规范（error_convert.rs:7-32）。

**方案**：admin 面统一走脱敏映射（动作导向短句 + 细节进 tracing）；ConfigError 透传改为固定文案 + 细节落日志。
补泄漏断言测试（仿 error_convert.rs:322/336/349 的既有模式）。

**验证**：新测试断言响应不含路径/内部细节；`grep -n 'format!("...: {e}' admin.rs` 清零。

### B-4: INV-6 路由切换窗口（设计项）

**问题**：路由表切换窗口无 fence/epoch——已 route 未 commit 的 in-flight 写语义未定义
（`docs/docs/reference/invariants.md:724-729` 登记 open）；现有测试用固定 300ms sleep（lib.rs:2488/:2538），
自认 flaky 模式。这是 G1 唯一未闭环的正确性契约缺口。

**方案**（两选一，先设计评审再动手）：
1. 为路由切换引入 epoch/fence：切换后旧 epoch 的写被明确拒绝或重试，语义写进 invariants.md；
2. 或正式降级为"已接受风险"，写明触发条件与影响面，删除 flaky 测试改确定性等待。

**验证**：方案 1 需 TLA 或确定性集成测试覆盖切换窗口；方案 2 需 invariants.md 状态从 open 改为 accepted-risk。

---

## Phase C — 可观测性欠账（G3-观测）

### C-1: formation 生命周期观测与自省

**问题**：`crates/rr-server/src/formation.rs`（1272 行，P1 旗舰特性）全文件零日志零指标；
operator 无法查询 genesis/forming/active 状态与 CAS revision——Forming 中的集群是运维黑盒，
/ready 只有一个 phase 字符串。

**方案**：
1. 加指标：`rr_formation_cas_total{result}`、`rr_formation_state`（gauge，0=forming/1=active/2=degraded）、
   `rr_formation_duration_seconds`；关键状态迁移加 info 日志。
2. 加自省：`rr-admin` 增加 `formation status` 命令（读 lifecycle store 或新增 Admin RPC），
   输出模式/状态/CAS revision/roster 摘要。
3. playbook 排错章节补充对应指引。

**验证**：集成测试断言 genesis 流程后指标存在且值正确；`just genesis-up` 后 `rr-admin formation status` 输出 Active。

### C-2: 快照传输与成员变更指标

**问题**：InstallSnapshot 无字节/耗时指标（接收侧仅 DEBUG 日志 raft_service.rs:144-149，
`rr_snapshot_installs_total` 只事后计数）；成员变更/leader 转移/手动快照有审计无指标（admin.rs 无 metrics 调用）。

**方案**：`rr_snapshot_transfer_bytes` / `rr_snapshot_transfer_duration_seconds`（发送+接收双侧）；
`rr_admin_membership_changes_total{op,result}`、`rr_admin_transfer_leader_total{result}`。

**验证**：快照追赶集成测试中断言指标递增；metrics-reference.md 同步登记。

### C-3: 观测文档校准

**问题**：metrics-reference.md 漏记 2 个指标（`rr_deferred_pending_via_non_deferred_total` deferred.rs:124、
`rr_idempotency_namespace_rejects_total` service_adapter.rs:480）+ `rr_server_requests_total` 漏记
`idempotency_namespace_limit` outcome 值；deploy/observability/README.md 阈值描述与规则文件不符
（45k vs 实际 8000，rr-alerts.yml:55；0.5 vs 1 elections/min，:105）。

**方案**：补齐漏记指标与标签值；README 阈值改为引用规则文件或直接写正确值；
把"指标双向核对"脚本化（从代码抽取指标名 diff 文档）纳入 `just check` 或 A-1 的 nightly。

**验证**：核对脚本 exit 0；文档与 `rr-alerts.yml` 一致。

---

## Phase D — 性能治理（G2）

### D-1: 重采基线并更新对外数字

**问题**：`docs/docs/reference/benchmarks.md:56-63` 的 S2 表仍是批处理/扇出落地前的数字；
最近一次 prod 全矩阵为 2026-09-12/13，此后 37 个 commit（含 P0/P1 核心路径改动）无重采；
intro.md:50 对多组平顶的归因落后于 roadmap-100k 的修正结论。

**方案**：用 `scripts/perf-baseline.sh` + `prod-perf.sh` 在当前 main 重采（含批处理/扇出开/关矩阵）；
更新 benchmarks.md（含批处理数字）与 intro.md:50 归因表述；基线归档 `docs/perf/baselines/`。

**验证**：benchmarks.md 数字与本次原始数据一致；日期为本次采样日。

### D-2: 热路径微基准补齐

**问题**：dispatch/apply 管线、`ShardRouter::route`、`rr_core::batch` 拆包、ReadIndex、snapshot 均无 criterion 基准，
bench-compare 的 15% 门槛对这些热点无保护。

**方案**：为上述 5 条路径补 criterion target（benches/Cargo.toml 注册）；接入 A-1 的 `bench-gate`。

**验证**：`cargo bench -- --list` 含新 target；人为注入一个明显退化（如 route 里加克隆）bench-gate 报 FAIL。

### D-3: G2 决策点裁定（决策项，无需写码）

**问题**：① `BatchConfig::ordered` 旋钮（roadmap-100k.md:141-143 留作 v0.3.0 决策）；② 扇出后多组复测
（roadmap M3 未勾选项）；③ "单组 100k"是否仍为承诺目标——默认配置 34k 与目标差 3×。

**方案**：一页裁定注记（.sisyphus 或 ADR）：每个决策点给出"做/不做/何时做"与理由；若 100k 立场调整，同步 intro.md。

**验证**：注记文件存在，roadmap-100k.md 对应条目勾选或标注 deferred。

---

## Phase E — 易用性/文档债（G3-易用）

### E-1: deploy/compose 名实不符

**问题**：`deploy/compose/` 只有 config/node{1,2,3}.toml，**没有 docker-compose.yml**；
node1.toml:11 注释引用不存在的文件；CHANGELOG.md:21 与 intro.md:59 声称 compose 模板存在。新用户照走立即踩空。

**方案**（两选一）：① 补齐 docker-compose.yml（三节点 + 可选 etcd，复用现有 config），纳入冒烟验证；
② 或删除 compose 目录与全部文案引用，部署模板收敛为 k8s/helm 两条路径。

**验证**：方案① `docker compose up` 三节点选主成功；方案② 全仓 grep `compose` 无残留引用。

### E-2: Java SDK 故事断裂

**问题**：`docs/docs/guide/client-sdk.md` 全文未提 Java SDK；Java 缺 batching/channel-fanout（Rust client 独有，
rr-client/src/batch.rs、lib.rs:216/294）；sdks/java 4 个 gradle wrapper 文件 dirty 未提交（用户本地改动）。

**方案**：client-sdk.md 增加 Java SDK 小节（安装/快速开始/能力对照表/与 Rust client 的差距清单）；
wrapper 改动由用户决定提交或还原（不代办）；Java batching 是否补齐记入差距清单（不强迫本期实现）。

**验证**：文档站 client-sdk 页含 Java 小节；差距清单与代码实际能力一致。

### E-3: 连接类错误信息加处方

**问题**：rr-admin 连接超时（main.rs:87）、REPL member discovery 失败（main.rs:1524）、重连失败（main.rs:1564）
只报症状不给处方；对照标杆是启动安全门与写拒绝（带修复路径）。

**方案**：三处报错补充下一步指引（检查端口角色 client vs admin、进程是否存活、参考 playbook 排错章节）。

**验证**：单元测试断言错误文案含指引关键词。

### E-4: 文档基线回写

**问题**：① intro.md 未登记 formation 子系统（P1 已交付的大能力）；② rr-quality-hardening.md 状态表过期
（实际已完成）、rr-prod-perf.md 头部与 CONTEXT.md 矛盾；③ tests/crash/src/lib.rs:27-32 注释已被 A-0016 修复推翻；
④ examples 仅 counter 有 README。

**方案**：intro.md 增加 formation 能力一段；两份 plan 状态表回写实际状态；crash 注释更新为当前事实；
至少为 kv-store 补 README（主入口示例）。

**验证**：四处文件更新；plan 文件状态与 CONTEXT.md 不再矛盾。

---

## Phase F — 工程治理（低优先）

### F-1: v0.3.0 范围裁定（轻量）

**问题**：原 phase4 内容已提前并入 v0.2.0，v0.3.0 无范围基线；Unreleased 持续累积变更无 release 归属。
（个人项目裁定后降级为卫生项，不构成风险。）

**方案**：一页注记列出 v0.3.0 候选（A-0017 裁定、ordered 旋钮、版本 bump、Java SDK 版本同步）；
发版前做一次 `version` bump + 迁移注记即可。

**验证**：注记文件存在。

### F-2: 发版前版本卫生 checklist

**问题**：个人项目无外部用户，破坏性窗口可长期开放；但发版瞬间需要一份核对清单避免漏掉版本号与迁移说明。

**方案**：在 release 文档补 checklist：workspace version bump、sdks/java version 同步、
CHANGELOG Unreleased 切段、破坏性变更迁移注记汇总。

**验证**：checklist 存在。

---

## 执行顺序建议

1. **先做 A-1**（执法恢复）——没有它，后续所有改动都在无门禁状态下合入。
2. B-1/B-2/B-3 是小型加固（各约半天量级），可紧随。
3. C-1 是 P1 成果的运维闭环，建议尽早。
4. D-3 是决策项，不阻塞其他工作，随时可做。
5. B-4（INV-6）是唯一需要设计评审的项，单独立项。
6. E/F 为收尾批，可打包一次做完。

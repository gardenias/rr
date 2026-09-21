---
title: "Change-membership server-side quorum guard"
status: implemented
created: 2026-08-28
updated: 2026-08-29
tags: [adr, safety, membership, quorum, admin, audit]
---

# A-0012 — Change-Membership Server-Side Quorum Guard

> 工作项:roadmap WS-6(`.sisyphus/plans/rr-v0.2.0-release-roadmap.md`)。
> 依据:全量 review P0 #7 后半(`.sisyphus/evidence/rr-full-review-2026-08-16.md`,HEAD=`ba69cc8`)。

## Context

rr 的 membership 变更入口有三层:

1. **AdminService RPC**:`AdminServiceImpl::change_membership`(`crates/rr-server/src/admin.rs:535-600`)是唯一的 operator-facing 服务端入口,当前唯一的校验是 `voter_node_ids` 非空(`admin.rs:552-565`,返回 `Status::invalid_argument`),随后直接调用 `RaftGroupManager::change_membership`(`admin.rs:566-569`)。
2. **Manager 层**:`RaftGroupManager::change_membership`(`crates/rr-raft/src/group_manager.rs:330-350`)是 openraft 的薄封装,`raft.change_membership(members, /* retain */ true)` 只做 group 存在性检查,无任何 quorum 策略。
3. **Client 侧 workflow**:`rr-admin node decommission`(`crates/rr-admin/src/decommission.rs`)在摘除前做 client-side 检查:`remaining.len() < MIN_VOTERS_AFTER_REMOVAL`(= 2,`decommission.rs:57`,检查点 `:174`)则放弃该 group(`GroupAction::AbortedQuorumLoss`)。

openraft 的 joint consensus 保证 membership 变更过程的**正确性**(不产生 split-brain),但不保证**可用性**——把 voter 集合缩减到 1 个是合法但脆弱的(SPOF),缩到 0 个等于拆掉 group。这类"可用性自杀"防护不属于 openraft 的职责,必须由 rr 自己执法。

另一个入口是 config watch loop:`RoutingTableChange::NodeRemoved` 会自动 `change_membership(group_id, replicas - nid)`(`crates/rr-server/src/node.rs:340-372`)。这是 routing-table 驱动的自动化路径,语义与 operator RPC 不同(见 Options)。

## Problem

1. **服务端无 quorum-loss 守卫**。任何能到达 admin port 的客户端(不经过 `rr-admin` 的 decommission workflow)都可以直接调 `ChangeMembership` 把 3-voter group 缩到 1 个 voter,服务端不拒绝。client-side 检查只是 advisory——这正是 review 元根因"不变量存在,但 layer Y 没强制"的实例。
2. **策略常量住错层且不可共享**。`MIN_VOTERS_AFTER_REMOVAL` 定义在 rr-admin(binary crate)里,服务端无法复用;策略随客户端版本漂移。
3. **文档在说谎**。`.sisyphus/CONTEXT.md:242` Wave 8 记载"rr-server: AdminServiceImpl 实现,含空 voter 拒绝、**quorum-loss check**"——代码中后者不存在(实测 `admin.rs:552` 仅查 empty)。该记载需在实施时一并修正。

## Options

### Option A(采纳):AdminService handler 内服务端守卫 + 共享策略 + `force` 逃生

在 `AdminServiceImpl::change_membership` 内、调用 manager 之前做守卫;策略常量与新错误 variant 放 rr-core;proto 增加显式 `force` 字段;force 路径强制 audit。client-side decommission 检查保留(UX 层 fail-fast)但改为引用共享常量。

- 优点:执法点在所有 operator 客户端的必经之路上;改动面小(admin.rs + error.rs + error_convert.rs + admin.proto + rr-admin);与现有 5-gate"配置不变量在 choke point 强制执行"的治理模式一致。
- 缺点:不覆盖 watch-loop 自动化路径(见 Option B 的取舍)。

### Option B(否决):守卫下沉到 `RaftGroupManager::change_membership`

在 manager 层守卫可覆盖所有调用方,包括 watch loop 的 `NodeRemoved` 自动摘除(`node.rs:340-372`)。否决理由:

- watch-loop 路径由 routing table(配置层单一事实源)驱动:若 manager 拒绝应用,raft membership 与 routing table 产生**持久分歧**,而该路径没有 `force` 语义,operator 的修复动作不明确——把一个可用性问题换成一个一致性问题。
- manager 是 openraft 的薄封装,把 operator 策略(含 force/audit 语义)塞进库层会污染 `rr-raft` 的边界;0.2.0 目标是堵住 operator 误操作,自动化路径的守卫需要单独设计(连同 R-2 TLA+ 建模一并考虑),列为 follow-up。

### Option C(否决):维持 client-side 检查,仅补文档

client-side 检查对任何直连 AdminService 的 gRPC 客户端无效;且 CONTEXT.md 的记载已经与代码脱节,继续文档先行只会加深漂移。

### Option D(否决):拒绝一切"缩减幅度超过当前 quorum"的变更

例如 5→2(一次摘 3 个)也拒绝。过度设计:joint consensus 已保证过程正确;真正需要守住的不变量是**变更后 voter 集合的最小可用规模**,而不是 delta。守卫目标集合规模还有一个结构性优点:并发两次 change_membership 各自看到旧视图也不会组合出违规——openraft 是 last-writer-wins 的全量替换语义,每个被应用的请求自身就保证结果 ≥ 2 voters(无 TOCTOU 组合风险)。

## Decision

采纳 Option A。具体设计如下。

### 1. 策略常量上移 rr-core

`MIN_VOTERS_AFTER_REMOVAL: usize = 2` 从 `crates/rr-admin/src/decommission.rs:57` 移到 rr-core(如 `rr_core::membership`),作为全仓唯一定义。rr-admin 增加对 rr-core 的依赖,decommission 删除本地副本、引用共享常量——其 client-side 预检查**保留**,因为它在 drain(TransferLeader + 10s 轮询)之前 fail-fast,是多 group 编排器的正确 UX;但它不再是唯一执法者。

### 2. 新错误 variant 放 `RaftError` 层

在 `crates/rr-core/src/error.rs` 的 `RaftError` 新增:

```rust
#[error("membership change rejected: target voter set has {target_voters} voter(s), \
         below minimum {min}; re-run with force to override")]
UnsafeMembershipChange { target_voters: usize, min: usize },
```

放在 `RaftError` 的理由:这是 raft 安全策略拒绝,与该 RPC 已返回的 `RaftError::GroupNotFound`/`NotLeader` 同层同级。在 `crates/rr-network/src/error_convert.rs` 映射为 `Status::failed_precondition`(先例:`RaftError::InitializationFailed` → failed_precondition,`error_convert.rs:88-94`;`RrError::IdempotencyExpired` → failed_precondition,`:52-59`)。voter 数量与 min 不是敏感信息,可回给客户端(`GetGroupMembership` 本就公开 voter 列表)。

时序约束:该 variant 必须在 WS-3 的 `#[non_exhaustive]` API freeze **之前**落地;freeze 后新增 variant 才不算 breaking。

### 3. 守卫位置、时机与 membership 视图

在 `AdminServiceImpl::change_membership`(`admin.rs:535`)内,顺序为:

1. group 存在性:`group_manager.get_raft(group_id)`,不存在 → `Status::not_found`(与 `get_group_membership` 的 `admin.rs:663-667` 一致);
2. 空集合检查(现有,`admin.rs:552`):无条件拒绝,**`force` 不能绕过**——n→0 是 teardown,走 `node-reset`(语义见 `decommission.rs:15-17`);
3. **新守卫**:读取 `raft.metrics().borrow_watched().membership_config.membership()` 的当前 voter 视图——与 `GetGroupMembership` 同源(`admin.rs:668-670`);若 `target_voters.len() < MIN_VOTERS_AFTER_REMOVAL` 且 `!force` → 拒绝;
4. 通过后才调用 `group_manager.change_membership`。

视图说明:change_membership 只有在 leader 上才能成功(follower 上 openraft 返回 `ForwardToLeader`,当前被 `group_manager.rs:342` 压成 `RrError::Application` → `Status::internal`),所以守卫在 leader 上读到的是已提交 membership,判定是权威的;在 follower 上读到旧视图也无所谓——后续 raft 调用必然失败,守卫只是提前给出更可读的错误。

### 4. 判定边界(以 3→2→1 为例)

| 变更 | 无 force | 有 force | 说明 |
|---|---|---|---|
| 3→2 | ✅ 允许 | ✅ | = MIN,多数派仍在 |
| 3→1 / 2→1 | ❌ `failed_precondition` | ✅ + audit | 结果 SPOF |
| 1→1(单节点换 voter) | ❌ | ✅ + audit | 已知取舍:单节点集群是 dev/teardown 场景,接受要求显式 force |
| n→0 | ❌ `invalid_argument`(现有检查) | ❌ 同左 | force 不绕过;teardown 走 node-reset |
| 2→3 等扩容 | ✅ | ✅ | 只守卫下限,不限制扩容 |

### 5. `force` 逃生参数与强制 audit

- proto3:`ChangeMembershipRequest` 增加 `bool force = 3;`(`crates/rr-network/proto/admin.proto:120-123`,当前只有 `group_id = 1` / `voter_node_ids = 2`)。proto3 增字段是 wire-additive,不构成 wire breaking。
- CLI:`rr-admin group change-membership` 增加 `--force` flag(`crates/rr-admin/src/main.rs:239-246` 的 `GroupCommands::ChangeMembership`,调用点 `:674-708`),透传到 request。
- decommission **不**增加 force 通道:它的契约维持"先加 voter 再摘除"(`decommission.rs:12-17`)。
- audit:拒绝走现有 `result = "failed"` 事件(带 reason);**force 放行必须额外产生一条 warn 级 audit 事件**,字段至少含 `op = "ChangeMembership"`、`force = true`、`result = "force_accepted"`、`peer`、`group_id`、`target_voters`,使所有被强制绕过的守卫在 `AUDIT_TARGET` 日志流中可 grep。

### 6. 错误如何返回 admin CLI

`RaftError::UnsafeMembershipChange` → `Status::failed_precondition` → `rr-admin` 的 `admin_action_outcome` 统一格式化为 `success=false message=...`(human)或 JSON(`main.rs:696-707` 现有路径),exit code 非零。运维脚本可凭 gRPC code 区分"守卫拒绝"(failed_precondition,可用 --force 重试)与"raft 失败"(internal)。

### 7. 滚动升级注意事项

proto3 默认丢弃未知字段:新客户端对**旧 server** 发 `force=true` 会被静默降级为无 force——而旧 server 本来就没有守卫,变更会被直接应用。守卫只有在 leader 跑新二进制后才生效;实施时需在 release notes 写明:滚动升级期间不要依赖该守卫。

### 8. 文档修正(follow-up)

实施 PR 必须同时修正 `.sisyphus/CONTEXT.md:242` Wave 8 的不实记载(删除或改为"空 voter 拒绝;quorum-loss check 见 A-0012"),并同步 rr-admin CLI 帮助文本与 `docs/` 中 change-membership 相关页面。

## Consequences

### Positive

- "结果 voter 数 ≥ 2" 这条不变量在 operator 必经的服务端 choke point 被强制执行,不再依赖客户端自觉。
- 策略常量全仓单一定义,client/server 不再漂移。
- force 逃生保留灾难恢复能力(如多数节点永久丢失后重建),且每次使用都有 audit 痕迹。
- 错误分类清晰:守卫拒绝 = `failed_precondition`(可重试/可 force),区别于参数错误与 raft 内部错误。

### Negative

- 直接调 RPC 的存量自动化脚本若会把 group 缩到 1 voter,0.2.0 起会被拒绝——需要加 `force`。
- AdminService handler 多一次 `raft.metrics()` 读(廉价,watch channel borrow)。
- watch-loop 的 `NodeRemoved` 自动摘除路径仍无守卫(已知缺口,见 follow-up)。

### Neutral / 对 v0.2.0 的 breaking 判定

- **行为 breaking(窄面)**:`ChangeMembership` 会把 voter 缩到 < 2 的请求,0.1.0 时代成功、0.2.0 起返回 `failed_precondition`。可接受:0.2.0 尚未发布(crates 仍为 0.1.0,freeze 在 WS-3),且这是安全修复;必须进 CHANGELOG 的 0.2.0 breaking 清单。
- **wire 兼容**:`force` 字段 proto3 additive,新旧客户端/服务端任意组合可互通(但见 Decision §7 的降级注意)。
- **API 兼容**:`RaftError` 新增 variant 在 `#[non_exhaustive]` freeze(WS-3)之前落地,不算 post-freeze breaking。

### 配套测试方案

1. **单元测试(rr-core)**:守卫谓词真值表——3→2 允许;3→1、2→1、1→1 拒绝;n→0 无条件拒绝(含 force);force 对 ≥1 目标放行;扩容不受限。
2. **单元测试(rr-server::admin)**:用 MemStorage 单节点 group 起 `AdminServiceImpl`,断言守卫拒绝返回 `Code::FailedPrecondition` 且 message 含 voter 数与 min;`force=true` 时请求穿透到 manager;空集合仍是 `Code::InvalidArgument`(force 不绕过)。
3. **集成测试(tests/integration,验收标准)**:3 节点 real-network 集群(harness 见 `tests/integration/src/lib.rs:210+`,节点以 `.with_allow_insecure(true)` 构造,`:279`/`:576`),经 `AdminServiceClient`(先例:`tests/integration/src/tls_auth_e2e.rs:34`)对 leader 调用:
   - `change_membership(voters=[1])`(即从 3 voter 一次摘除 2 个)→ 断言 `FailedPrecondition`,随后 `GetGroupMembership` 确认 membership 未变;
   - `change_membership(voters=[1,2])` → 成功;
   - `change_membership(voters=[1], force=true)` → 成功,`GetGroupMembership` 确认生效;
   - 对 follower 调用 → 非守卫错误(not leader 路径不回归)。
4. **audit 测试**:tracing test subscriber 捕获 `AUDIT_TARGET`,断言 force 路径产出 `force=true` 的 warn 事件、拒绝路径产出 `failed` 事件。
5. **rr-admin CLI 测试**:`--force` flag 解析(参照 `main.rs:1420-1438` 现有 parse 测试模式);decommission 单测保持绿(常量改引用不改值,`decommission.rs:396-398` 的常量值断言同步改为引用共享常量)。

## Changelog

| Date | Change |
|------|--------|
| 2026-08-28 | Initial: server-side quorum guard for `ChangeMembership`(target-voter 下限 2)、`force` proto/CLI 逃生 + 强制 audit、`MIN_VOTERS_AFTER_REMOVAL` 上移 rr-core、`RaftError::UnsafeMembershipChange` → `failed_precondition`;否决 manager 层下沉(Option B)、client-only(Option C)、delta 守卫(Option D);明确滚动升级降级风险与 CONTEXT.md Wave 8 不实记载的修正义务。 |
| 2026-08-29 | Implemented(WS-6):按 Option A 落地。守卫在 `crates/rr-server/src/admin.rs:537-638`,判定顺序为 group 存在性(not_found)→ 空集合(invalid_argument,`force` 不可绕过)→ quorum guard;`force` 为 wire-additive proto 字段,force 放行产 warn 级 `force_accepted` audit 事件;`rr_core::membership::MIN_VOTERS_AFTER_REMOVAL` 为全仓单一定义(`crates/rr-core/src/membership.rs:19`,rr-admin 删除本地副本改引用);`RaftError::UnsafeMembershipChange` 经 `error_convert.rs:103` 映射为 `Status::failed_precondition`。测试:rr-core 谓词真值表(`membership.rs` 测试组)+ admin handler 守卫测试组(`admin.rs:1119-1260`,含边界放行/force 穿透/两条 audit 断言)+ `tests/integration/src/membership_guard.rs:166` e2e(3→1 拒绝 / 3→2 放行 / 2→1 force 放行)。watch-loop `NodeRemoved` 自动摘除路径的守卫按 Option B 决议保持 follow-up(连同 R-2 TLA+ 建模)。 |
| 2026-09-21 | Follow-up 闭环:watch-loop `NodeRemoved` 自动摘除路径(`crates/rr-server/src/node.rs` 的 `RoutingTableChange::NodeRemoved` 分支)接入同一谓词 `membership_change_is_safe`——剔除后目标 voter 数 < `MIN_VOTERS_AFTER_REMOVAL` 时拒绝该次自动变更并 `warn!` 留痕(组保持不变)。自动化路径刻意无 `force` 通道:自动事件不携带 operator 意图,刻意缩到 quorum 以下须走 `change-membership --force` / `node-reset`。维持 Option B(守卫不下沉 manager 层,避免 raft membership 与 routing table 持久分歧)。测试:`tests/integration/src/lib.rs::test_node_removed_quorum_guard`(2-voter 组 NodeRemoved 后 membership 不变且仍可写;摘掉守卫的负面对照实测变红)。TLA+ `MembershipGuard.tla` 性质不变(watch 路径为第三入口,策略同构)。 |

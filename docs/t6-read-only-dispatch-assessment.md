# T6 — 只读分发能力评估（read-index / lease-read）

> 任务卡：rr-iteration T6。范围 = **能力评估，产出结论即可**，实施另计。
> 评估日期：2026-09-18。基线：rr `iter/r0-persistence`（含 T1 持久化 / T2 ApplyContext / T3 Deferred Response）。

## 任务卡原始陈述

> `Service` trait 仅共识 `handle`，无只读分发——my-biz 的 QueryBook read-index 因此受阻（2026-09-05 记录归档）。
> 评估 read-index / lease-read 支持的成本与形态，产出结论（做/不做/怎么做）。

## 结论（先行）

**做，但大部分能力已存在——my-biz 的 QueryBook read-index 今天就能落地，无需 rr 改动。**
「`Service` trait 无只读分发」的原始陈述**已不准确**：rr 在 `RaftGroupManager` 层提供了两条只读路径，
openraft 底层同时支持 `ReadIndex` 与 `LeaseRead`。真正的残余缺口很小（lease-read 未暴露、
Service trait 读写在类型层不分离），均属低成本增量，不构成阻塞。

## 现状精确刻画（代码事实）

### 已有的只读路径（rr 框架层，`crates/rr-raft/src/group_manager.rs`）

1. **`local_read(group_id, payload)`（`group_manager.rs:862`）**：直接读本地 SM 副本（经 `read_handle`），
   **stale**——可能落后于 leader 的 commit/apply 进度。无一致性保证，最快。
2. **`linearizable_read(group_id, payload)`（`group_manager.rs:898`）**：走 openraft
   `get_read_linearizer(ReadPolicy::ReadIndex)`——leader 先向 quorum 确认任期内的 commit frontier
   （ReadIndex），等本地 SM apply 追平该点后再读。**线性一致**。非 leader 调用时把
   `ForwardToLeader` 摊平成 `RaftError::NotLeader{leader, leader_addr}`，dispatch/SDK 层据此重定向到 leader。
3. **读分发机制**：`ServiceStateMachineAdapter` 暴露 `read_handle() -> StateMachineReader`
   （`service_adapter.rs` 的 `ServiceReadHandle`），其 `read(payload)` 复用 `Service::handle` 处理查询。
   `local_read` / `linearizable_read` 都经此。

### openraft 底层（vendor 0.10-alpha，`src/raft/mod.rs:247`）

- `ReadPolicy::ReadIndex`——quorum 确认，最强一致，每次读一次网络往返。
- `ReadPolicy::LeaseRead`——leader 凭租约本地直读，免 quorum 往返，延迟更低；一致性略弱
  （假设时钟漂移可忽略）。**openraft 已内置支持**。

## 与原始陈述的差异

- 「`Service` trait 仅共识 handle、无只读分发」**字面成立但误导**：`Service` 确实只有一个 `handle`
  （读写在类型层不区分），但**功能上只读分发存在**——查询经 `read_handle` 走 `Service::handle`，
  由 `local_read`/`linearizable_read` 提供 stale 与 ReadIndex 两种一致性档位。所以 my-biz 的
  QueryBook read-index **在框架层并不受阻**：直接调 `RaftGroupManager::linearizable_read` 即可。
- 真正的缺口是两个**小的**：
  - **G1 lease-read 未暴露**：rr 的 `linearizable_read` 写死 `ReadPolicy::ReadIndex`；openraft 的
    `LeaseRead` 没有接出。成本 = 极低（暴露一个 read-policy 参数，或加 `lease_read()` 方法）。
  - **G2 读写在 Service 类型层不分离**：读与写都过 `Service::handle`，「只读命令」只是约定而非类型约束。
    纯人体工学问题，不阻塞。

## 成本与形态

| 项 | 成本 | 形态 | 建议 |
|---|---|---|---|
| QueryBook read-index（ReadIndex 线性一致读） | **零 rr 改动** | 直接用 `RaftGroupManager::linearizable_read` | **现在做**（T7 接线时用） |
| G1 lease-read | **极低**（openraft 已支持；rr 暴露 `ReadPolicy::LeaseRead`，或在 `linearizable_read` 加 policy 参数） | `lease_read(group_id, payload)`，leader 本地直读 | **做**——若 QueryBook 读延迟敏感（ReadIndex 的 quorum 往返在热点上可观）。单机部署下 ReadIndex≈LeaseRead（quorum=1），差异主要体现在多副本 |
| G2 Service 读写类型分离 | 低-中（加 `read`/`write` 分型或 `ReadOnlyService` 标记） | 类型层约束只读命令不 mutate | 可选；pre-release 不紧急，随读模型演进再议 |

## 对 T7 的直接指引

- my-biz 的 QueryBook / 只读查询路径：**用 `linearizable_read`（ReadIndex）**，零 rr 改动。
- 若后续读延迟成为瓶颈：把 G1（lease-read）作为一个小增量接出，单独立项即可，不属本任务栈硬前置。

## 残余 / 不做

- **不做** lease-read 的本期实施：G1 是独立小增量，且不阻塞 my-biz（ReadIndex 已够用）。
- **不做** 读写在 Service trait 的类型层强制分离（G2）：约定 + 评审足以覆盖，类型化是锦上添花。

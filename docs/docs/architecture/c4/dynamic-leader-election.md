---
sidebar_position: 9
---

# Dynamic — Raft Leader Election

领导者选举发生在**每个 Raft 组内部独立**进行。当一个 group 的 leader 失联（进程崩溃、网络分区、GC 暂停），其他成员的选举定时器会超时并发起选举。本图展示标准的 Raft 选举流程在 rr 中的具体实现。

---

Leader election happens **independently inside each Raft group**. When a group's leader becomes unreachable (process crash, network partition, GC pause), other members' election timers fire and trigger a vote. This diagram shows how the standard Raft election flow is realized in rr.

## 高层流程（C4 Dynamic）/ High-Level Flow (C4 Dynamic)

```plantuml
@startuml
!include <C4/C4_Dynamic.puml>

title Dynamic Diagram - Raft Leader Election (Group 7)

Container_Boundary(n1, "Node 1 (was Leader, now unreachable)") {
  Component(n1_raft, "RaftNode G7", "rr-raft", "Lost heartbeat to followers")
}

Container_Boundary(n2, "Node 2 (Follower → Candidate → Leader)") {
  Component(n2_raft, "RaftNode G7", "rr-raft", "openraft core")
  Component(n2_sm, "StateMachine G7", "rr-raft::sm_adapter", "User state machine")
}

Container_Boundary(n3, "Node 3 (Follower)") {
  Component(n3_raft, "RaftNode G7", "rr-raft", "openraft core")
}

Container_Ext(client, "rr-client", "Rust library", "User application")

Rel(n2_raft, n2_raft, "1. Election timeout → Candidate, term += 1")
Rel(n2_raft, n3_raft, "2. VoteRequest(term, last_log_idx, last_log_term)", "gRPC")
Rel(n2_raft, n1_raft, "3. VoteRequest (may fail - unreachable)", "gRPC")
Rel(n3_raft, n2_raft, "4. VoteResponse(granted=true)", "gRPC")
Rel(n2_raft, n2_raft, "5. Quorum (2/3) → Leader")
Rel(n2_raft, n3_raft, "6. AppendEntries heartbeat", "gRPC")
Rel(client, n2_raft, "7. Clients learn the new leader via leader_info redirects (routing table never carries leader state)", "gRPC")

@enduml
```

## 详细时序图 / Detailed Sequence

```plantuml
@startuml
autonumber
participant N1 as "Node 1 (Leader,\nthen crashes)"
participant N2 as "Node 2 (Follower)"
participant N3 as "Node 3 (Follower)"
participant Client as "rr-client\n(user app)"

note over N1, N3 : Normal steady state: N1 sends heartbeats at interval T
N1 -> N2 : AppendEntries (empty, heartbeat)
N1 -> N3 : AppendEntries (empty, heartbeat)
N2 --> N1 : ack
N3 --> N1 : ack

note over N1 : 💥 N1 crashes / network partitions

note over N2 : Election timeout (randomized 150-300ms)\nfires first on N2
N2 -> N2 : state = Candidate\ncurrent_term += 1\nvoted_for = self
N2 -> N3 : VoteRequest(term, last_log_idx, last_log_term)
N2 -> N1 : VoteRequest (connection fails — N1 down)

note over N3 : term > my current_term ✓\nlog at least as up-to-date ✓\nhaven't voted this term ✓
N3 -> N3 : current_term = N2.term\nvoted_for = N2
N3 --> N2 : VoteResponse(granted=true)

note over N2 : Votes: self(1) + N3(1) = 2/3 → Quorum
N2 -> N2 : state = Leader\ninitialize next_index[], match_index[]

N2 -> N3 : AppendEntries (empty, heartbeat with new term)
N3 --> N2 : ack

note over N2, Client : Nothing writes the new leader to ConfigProvider —\nthe routing table has no leader field (ADR A-0013).\nGetClusterInfo reports runtime topology (leader from raft metrics),\nbut clients learn the leader via redirects (the SDK does not poll it).

note over Client : Next request lands on old leader (N1)\n→ connection fails\n→ client tries next bootstrap → N3\n→ N3 returns a redirect response (success=false + leader_info{N2})
Client -> N3 : ClientRequest(partition=42)
N3 --> Client : ClientResponse{success=false, leader_info={node_id=2, addr=host-2:51001}}
Client -> N2 : ClientRequest (retry, after backoff)
N2 --> Client : ClientResponse{success=true, payload}
@enduml
```

## 选举安全性保证 / Election Safety Guarantees

Raft 协议（由 openraft 实现）保证以下不变量：

The Raft protocol (implemented by openraft) guarantees the following invariants:

1. **At most one leader per term** — 一个 term 内最多一个 leader；通过 "majority vote required" 保证。
2. **Leader Completeness** — 新 leader 拥有所有之前 term 已 commit 的日志条目；通过 "log comparison in VoteRequest" 保证。
3. **Log Matching** — 如果两个日志在某个 index 上条目相同，那么之前的条目也相同。

rr **不会** 削弱这些保证——它原封不动地使用 openraft，仅在此之上加多组管理与网络层。

---

rr **does not** weaken these guarantees — it uses openraft as-is, adding multi-group management and networking on top.

## 时间参数 / Timing Parameters

| Parameter | Typical Value | Tradeoff |
| :--- | :--- | :--- |
| `heartbeat_interval` | 50 ms (rr default) | Lower = faster failure detection, higher = less network chatter |
| `election_timeout_min` | 150 ms (rr default) | Must be > 2× heartbeat_interval |
| `election_timeout_max` | 300 ms (rr default) | Randomized per-node to prevent split votes |
| `snapshot_policy` | `SnapshotPolicy::LogsSinceLast(50_000)` (rr default, `rr_raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST`) | Entries logged since last snapshot before triggering a new one |

**错误配置的后果 / Consequences of misconfiguration**:
- `election_timeout < heartbeat_interval` → 不必要的选举，集群颠簸
- `election_timeout ≥ client_timeout` → 客户端在选举完成前就返回错误
- `heartbeat_interval > RTT * 2` → 正常运行中也会触发选举

---

- `election_timeout < heartbeat_interval` → spurious elections, cluster flapping
- `election_timeout ≥ client_timeout` → clients error out before election completes
- `heartbeat_interval > RTT * 2` → elections triggered even during normal operation

## 选举风暴 / Election Storms

如果多个 follower 同时超时并发起选举，可能会出现**split vote**（没有候选者获得多数）。Raft 通过**随机化超时时间**（在 [min, max] 区间内均匀随机）来降低这种情况的概率。出现 split vote 时，下一轮会自然错开重试。

If multiple followers time out simultaneously and start elections, a **split vote** can occur (no candidate gets a majority). Raft mitigates this by **randomizing election timeouts** (uniform random within `[min, max]`). When a split vote happens, the next round naturally staggers.

rr 的实现中，每个 group 独立选举——**一个 group 的选举风暴不会影响另一个 group 的 leader 稳定性**。

In rr's implementation, each group elects independently — **an election storm in one group does not destabilize another group's leader**.

## 多组场景下的并发选举 / Concurrent Elections in Multi-Group

当整个节点崩溃时，**N 个 Raft 组同时失去一个成员**——如果这个节点是多个组的 leader，多个组会并发发起选举：

When an entire node crashes, **N Raft groups simultaneously lose a member** — if that node was the leader of multiple groups, multiple elections start concurrently:

```plantuml
@startuml
left to right direction

package "Node 2 crashes (was leader of G1, G3, G7)" {
  rectangle "t=0: Node 2 down" as t0
}

package "Concurrent elections" {
  rectangle "G1: N1 → Leader" as g1
  rectangle "G3: N3 → Leader" as g3
  rectangle "G7: N1 → Leader" as g7
}

t0 --> g1
t0 --> g3
t0 --> g7
@enduml
```

rr 通过 **独立的 openraft 任务和独立的随机超时**，让每个 group 的选举互不干扰。但要注意：
- **网络资源竞争**：选举风暴期间，所有 group 的 VoteRequest 共享同一个 ChannelPool。
- **CPU**：每个 group 的日志比较、签名验证并行执行——多核 CPU 能平滑此负载。

---

rr, via **independent openraft tasks and independent randomized timeouts**, keeps each group's election independent. Note however:
- **Network resource contention**: during election storm, all groups' VoteRequests share one ChannelPool.
- **CPU**: log comparison, signature verification run in parallel per group — multi-core CPUs absorb this.

## 相关 / Related

- 客户端如何感知 leader 变化：[dynamic-put-request.md](./dynamic-put-request.md)
- 路由表变更（不含 leader——leader 是 raft 运行时状态）：[dynamic-config-update.md](./dynamic-config-update.md)

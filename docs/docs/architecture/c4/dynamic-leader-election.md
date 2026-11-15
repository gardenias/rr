---
sidebar_position: 9
---

# 动态：Raft 领导者选举 / Dynamic: Raft Leader Election

每个 Raft group 独立选举。leader 失联后，其他成员的随机选举定时器超时并发起投票。

Each Raft group elects independently. When its leader becomes unreachable, followers' randomized election timers
expire and start an election.

## 高层流程 / High-level Flow

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

## 详细时序 / Detailed Sequence

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

note over N1 : N1 crashes / network partitions

note over N2 : Election timeout (randomized 150-300ms)\nfires first on N2
N2 -> N2 : state = Candidate\ncurrent_term += 1\nvoted_for = self
N2 -> N3 : VoteRequest(term, last_log_idx, last_log_term)
N2 -> N1 : VoteRequest (connection fails — N1 down)

note over N3 : term > my current_term: OK\nlog at least as up-to-date: OK\nhaven't voted this term: OK
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

## 安全性保证 / Safety Guarantees

openraft 实现 Raft 不变量：同一 term 最多一个 leader；新 leader 必须包含此前已提交日志；相同 index 和
term 的日志条目具有相同前缀。Rr 只增加多组管理和网络层。

Openraft implements Raft's invariants: at most one leader per term, a new leader contains all previously committed
entries, and matching index/term entries imply matching prefixes. Rr adds multi-group management and networking
without changing those rules.

## 时间参数 / Timing Parameters

| 参数 / Parameter | 默认值 / Default | 说明 / Meaning |
| :--- | :--- | :--- |
| `heartbeat_interval` | 50 ms | leader 心跳间隔 / Leader heartbeat interval |
| `election_timeout_min` | 150 ms | 随机选举超时下限 / Lower election-timeout bound |
| `election_timeout_max` | 300 ms | 随机选举超时上限 / Upper election-timeout bound |
| `snapshot_policy` | `SnapshotPolicy::LogsSinceLast(50_000)` | 距上次快照的日志阈值 / Entries since last snapshot |

选举超时应为心跳和网络延迟留出余量；客户端超时应覆盖可预期的选举窗口。过小的选举超时会造成无必要
的换主，过短的客户端超时会在选举完成前返回错误。

Election timeouts need headroom over heartbeats and network latency; client timeouts should cover the expected
election window. Too-small election timeouts cause unnecessary leadership changes, while too-short client timeouts
return errors before election completes.

## Split Vote 与多组选举 / Split Votes and Multi-group Elections

多个 candidate 同时参选可能都没有多数票。`[min, max]` 区间内的随机超时会错开下一轮。group 之间任务和
定时器独立，但 VoteRequest 仍共享连接池、CPU 和网络资源。

Simultaneous candidates can each fail to win a majority. Randomized timeouts in `[min, max]` stagger the next
round. Groups have independent tasks and timers, but VoteRequests still share channel pools, CPU, and network
resources.

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

## 相关 / Related

- [客户端发现 leader / Client leader discovery](./dynamic-put-request.md)
- [路由表不含 leader / Leaders stay out of routing tables](./dynamic-config-update.md)

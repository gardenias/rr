---
title: "Remove GroupMeta.leader — volatile raft runtime state does not belong in the routing table"
status: implemented
created: 2026-08-28
updated: 2026-08-29
tags: [adr, routing, leader, client-redirect, breaking-change, v0.2.0]
---

# A-0013 — 删除 `GroupMeta.leader`:leader 发现归位到 client redirect 缓存

## Context

`RoutingTable`(`crates/rr-core/src/types.rs:29-37`)是 rr 的配置面数据模型：由
`ConfigProvider`(`crates/rr-core/src/config.rs:46`)提供，描述 partition → group →
replicas 的放置关系。它有两个实现：

- `MemConfigProvider`(`crates/rr-config/src/mem.rs`)——测试/开发用内存实现;
- `EtcdConfigProvider`(`crates/rr-config-etcd/src/lib.rs`)——生产实现，key schema
  由 A-0004 定义。

`GroupMeta`(`crates/rr-core/src/types.rs:22-26`,struct 无文档)目前定义为:

```rust
pub struct GroupMeta {
    pub group_id: GroupId,
    pub leader: Option<NodeId>,   // ← 本 ADR 删除的字段
    pub replicas: Vec<NodeId>,
}
```

关键事实:**这个 `leader` 字段从来不是 raft 选举的结果**。两个 provider 全部用
`replicas.first().copied()` 填充(`mem.rs:54`、`mem.rs:103`、`rr-config-etcd/src/lib.rs:404`),
即"replicas 数组首元素"这一静态启发式;`update_group_membership` 在 membership 变更时
还会把 `leader` 重置回新的 `replicas[0]`(`mem.rs:97`)。真实的 leader 存在于 raft 运行时
(openraft `metrics.current_leader`,如 `crates/rr-server/src/admin.rs:157`/`181` 所示),
两者没有任何同步通道。

与此同时,client SDK 已经具备完整的 leader 发现机制,**不经过 routing table**:

- wire 上:`ClientResponse.leader_info`(`crates/rr-network/proto/client.proto:62`)
  与 `ReadLocalResponse.leader_info`(`client.proto:102`)承载 `LeaderInfo{node_id, addr}`
  (`client.proto:71-74`);
- server 端:写路径把 openraft `ForwardToLeader` 实时翻译为 redirect
  (`crates/rr-server/src/dispatch.rs:115-122`);linearizable read 路径把
  `RaftError::NotLeader { leader, leader_addr }` 翻译为 redirect(`dispatch.rs:166-173`,
  机制见 A-0009 的 2026-05-13 changelog);
- client 端:`RrClient.leader_cache`(`crates/rr-client/src/lib.rs:88`)按 group 缓存
  `(NodeId, addr)`,收到 redirect 时更新并重试(`lib.rs:584-602`),连接失败或 RPC 失败时
  失效(`lib.rs:540`、`lib.rs:620`)。Java SDK 有同构实现
  (`sdks/java/.../client/LeaderCache.java`)。

## Problem

### 静态配置表携带易变运行时状态是系统性错误

leader 是 raft 组的**运行时易变状态**:选举、网络分区、节点重启、transfer-leadership
都会改变它,时间尺度是秒级。routing table 是**配置面数据**:由控制面写入、provider
缓存、watch 分发,更新时延与选举时延不在同一量级,而且**没有任何 writer 负责把选举
结果写回 config store**。把前者放进后者,违反单一事实源(single source of truth):
同一条信息出现了两个"权威"副本,其中一个是永久 stale 的。

### 具体 stale 场景

1. **初始即错**。`add_partition(group=100, replicas=[1,2,3])` 后,`GroupMeta.leader`
   被填为 `Some(1)`(`mem.rs:52-56`)。但 raft 选举结果完全可能是 node 2——
   `tests/integration` 里 3 节点选举只断言"leader 是 1..=3 之一"
   (`tests/integration/src/lib.rs:676-687`),从不保证是 replicas[0]。字段在写入的
   那一刻就可能已经是错的。
2. **leader 切换后永久漂移**。node 1 宕机,node 2 当选新 leader。routing table 中的
   `leader` 仍是 `Some(1)`,直到某次 `update_group_membership` 把它重置为新 replicas
   数组的首元素——而这个值依旧是启发式猜测,不是选举结果。
3. **消费者把它当真**。`ShardRouter::get_leader`(`crates/rr-router/src/router.rs:55-65`)
   直接返回 `meta.leader`,`get_leader_addr`(`router.rs:72-79`)据此解析地址。任何
   依赖这两个方法做"先发请求给 leader"的调用方,拿到的是静态猜测值,会产生系统性
   的误判(把请求打到已下台的旧 leader,或永远偏向 replicas[0])。
4. **etcd 场景同样虚构**。`EtcdConfigProvider::get_routing_table` 从 etcd 读出
   replicas 数组后用同样的 `replicas.first().copied()` 现场编造 leader
   (`rr-config-etcd/src/lib.rs:400-407`);对没有 group key 的 partition 则填
   `leader: None`(`lib.rs:412-416`)——同一份 routing table 里"leader"一会儿是
   猜测值、一会儿是 None,语义不自洽。

### 为什么必须在 v0.2.0 前删除

`GroupMeta` 是 `rr-core` 的 `pub` 类型并经 `rr-core/src/lib.rs:76` re-export。0.2.0
是已声明的 breaking 窗口(CHANGELOG 已含同级 breaking;WS-3 做 API freeze)。freeze
之后删字段就是又一次 breaking;现在不删,这个错误抽象就会作为公开 API 被下游固化。

## Options Considered

### Option A — 删除字段,leader 发现统一走 client redirect 缓存(选定)

`GroupMeta` 只保留 `{ group_id, replicas }` 纯配置面元数据。leader 发现完全交给已
就位的 `NotLeader`/`LeaderInfo` redirect + client leader cache。随字段一并删除失去
数据源的 `ShardRouter::get_leader` / `get_leader_addr` 与其专属错误
`RouterError::NoLeader`。

Pros:

- 消灭双事实源;routing table 回归纯配置语义,永不 stale;
- client 获得的 leader 是**被 redirect 时的真实运行时 leader**,而非静态猜测;
- 与 0.2.0 已落地的 `leader_addr`(A-0009 follow-up)正好配套,替代机制零新增
  wire 变更;
- Rust/Java 两个 SDK 已是这个模型,行为不变。

Cons:

- breaking change:`rr-core` 公共类型删字段、`rr-router` 删两个 pub 方法、
  `rr-core` 删一个错误 variant,下游编译期报错(见"Breaking 判定与迁移指引")。

### Option B — 保留字段但标注"仅作 hint / 可能 stale"(否决)

Pros:非 breaking,改动最小。

Cons:

- "可能是错的权威值"比"没有值"更糟:调用方无法区分 hint 与事实,`get_leader`
  的返回类型 `Result<NodeId>` 在签名层面依然承诺"这是 leader";
- 字段无文档的现状(`types.rs:22-26` 无 doc comment)正是它被误用的原因之一,
  补文档不改变类型系统层面的误导;
- 单一事实源问题原样保留,stale 场景 1/2/4 全部照旧。Oracle 评审已明确:
  "删除,不是修启发式"(review 设计错误 #5 / Oracle Q3)。

### Option C — 由 ConfigProvider 动态维护 leader(watch raft 事件写回 config store)(否决)

让每个节点在 `on_leader_elected` / metrics ticker 里把选举结果写回 etcd/mem
provider,使 routing table 的 leader 接近真实。

Pros:routing table 继续提供 leader 查询能力。

Cons:

- **引入写路径与一轮新的分布式一致性问题**:多节点同时上报、旧 leader 与
  新 leader 短暂并存、写回失败重试,等于在 raft 之外再造一个弱一致的 leader
  registry——而 raft 本身已经是最强一致的 leader registry;
- `ConfigProvider` trait 当前刻意只读(A-0004:"Trait surface remains read-only"),
  此方案要求开写洞,破坏该架构约束;
- watch 分发时延 + 写回时延使其在选举风暴期间仍然 stale,只是把 stale 窗口
  从"永久"缩小到"不可界定";
- 与 client redirect 缓存相比,链路更长、故障面更大、收益为零。

### Option D — 改进启发式(如 bootstrap 顺序对齐、选举后一次性修正)(否决)

任何启发式的输出都不是选举结果,只是"更精致的猜测"。review 已确认该方向
(Oracle Q3),不重复论证。

## Decision

采用 **Option A**:从 `GroupMeta` 删除 `leader` 字段,并连带清理其唯一消费链。

### 类型与 API 变更

1. `GroupMeta` 变为 `{ group_id, replicas }`,并补上缺失的 rustdoc,明确它是
   **配置面静态元数据**,不含 raft 运行时状态;
2. 删除 `ShardRouter::get_leader`(`router.rs:55-65`)与
   `ShardRouter::get_leader_addr`(`router.rs:72-79`)——删除字段后这两个方法
   没有数据源,保留只会让错误抽象换个形式存活;
3. 删除 `RouterError::NoLeader { group_id }`(`crates/rr-core/src/error.rs:70-71`)
   ——其唯一生产者就是 `get_leader`;同步清理 gRPC 映射
   (`crates/rr-network/src/error_convert.rs:146` 及测试 `:268`);
4. `router.get_leader_addr.success` metric(`router.rs:77`)随方法一并移除,
   `metrics-reference.md` 中的条目同步删除。

以上全部落在 v0.2.0 breaking 窗口内,与 WS-3 的 API freeze 同步收口。

### 删除后的 leader 发现机制(替代路径,均已存在)

**写请求(client SDK)**:

```
首次请求: leader_cache miss → 按 bootstrap_addrs 轮询任一节
    → 若非 leader, server 由 openraft ForwardToLeader 得到真实 leader
      (dispatch.rs:115-122), 回 ClientResponse.leader_info{node_id, addr}
    → client update_leader_cache 并立即重试到该 addr (lib.rs:584-602)
后续请求: cache hit → 直连 leader (lib.rs:503-521)
leader 切换/节点宕机: 连接失败或 RPC 失败 → remove_cached_leader (lib.rs:540/620)
    → 回退 bootstrap 轮询 → 新一轮 redirect 收敛到新 leader
```

**linearizable read**:同一模型,server 端由 `RaftError::NotLeader { leader,
leader_addr }` 填充 `ReadLocalResponse.leader_info`(A-0009 follow-up,
`dispatch.rs:166-173`),client 按 `leader_info` 重试(`lib.rs:389-452`)。

**server 侧不再需要任何来自 routing table 的 leader 提示**。观测面(admin API)
展示真实 leader 的正确数据源是 raft 运行时 metrics:`admin.rs:157`/`181` 已用
`metrics.current_leader` 填充 `leader_node_id`,此路径与 `GroupMeta` 无关、保持
不变。`dispatch.rs:187-198` 的 `get_cluster_info` 目前给 `GroupInfo.leader_node_id`
填 `0` 占位;若未来要提供真实值,必须从 raft metrics 取,**不得**恢复路由表字段
(本 ADR 将该约束写入代码注释)。

### Wire / 持久化兼容与迁移

**结论先行:经源码核实,删除该字段对 etcd 存量数据与所有 wire 协议零影响,无需
数据迁移。** 任务前置假设("etcd 存量数据含 leader 字段")与代码事实不符,证据如下:

1. **etcd schema(A-0004)从不持久化 `GroupMeta` struct**。groups key
   `/rr/{cluster_id}/groups/{group_id}/replicas` 的 value 是 JSON `Vec<NodeId>`
   数组(解码点:`rr-config-etcd/src/lib.rs:399`;wire shape 测试:`lib.rs:595`
   的 `vec![1_u64, 2, 3]`)。`leader` 是读取时用 `replicas.first().copied()` **现场
   派生**的(`lib.rs:404`),从未落盘。nodes/partitions 两类 key 的 value
   (`NodeAddr`、`PartitionValue`,`lib.rs:28-32`)同样不含 leader。因此:
   - etcd 中无任何含 `leader` 键的存量 value,**无 serde 反序列化兼容问题**;
   - A-0004 的 key layout 与 value 编码**均不变**,schema 无需版本演进;需要更新
     的是描述 `GroupMeta` 内存模型的文档(见受影响清单),不是 A-0004 本身。
2. **`GroupMeta`/`RoutingTable` 的 serde derive 无实际序列化消费者**。全仓检索
   `serde_json::to_*` / bincode 调用点,没有任何位置把 `GroupMeta` 或 `RoutingTable`
   整体序列化到 wire 或磁盘。serde 默认行为(忽略未知字段;`types.rs:21` 的 derive
   无 `deny_unknown_fields`)构成理论兜底:即便下游曾自行把含 `leader` 键的旧 JSON
   反序列化为新 `GroupMeta`,也会静默忽略该键,不报错。
3. **gRPC proto 中没有 `GroupMeta` 消息**。节点间 raft RPC 是 openraft bincode
   payload(`crates/rr-server/src/lookup.rs`),client/admin 面是 proto,均不涉及
   `GroupMeta`。

**滚动升级期间新旧节点混布行为**:routing table 不经节点间 wire 交换,各节点独立
从同一 etcd key space 读取;旧代码读到 replicas 后派生 `leader = replicas[0]` 并仅在
`ShardRouter::get_leader*` 内消费,新代码无此路径。client SDK 从不读 routing table,
只靠 bootstrap + redirect,因此混布期间 client 行为完全一致。唯一不兼容面是**编译
期**:依赖被删方法/variant 的下游代码在升级依赖版本时报错,按迁移指引处理。

## 受影响调用点清单(全部已逐一核实)

### 生产代码(必须修改)

| 位置 | 现状 | 改法 |
|---|---|---|
| `crates/rr-core/src/types.rs:22-26` | `GroupMeta` 定义,`leader: Option<NodeId>` 在 :24,无文档 | 删字段;补 rustdoc 声明"配置面静态元数据,不含运行时 leader" |
| `crates/rr-config/src/mem.rs:52-56` | `add_partition` 以 `replicas.first().copied()` 填充 leader(:54) | 构造 `GroupMeta` 时去掉 `leader` 键 |
| `crates/rr-config/src/mem.rs:97` | `update_group_membership` 更新分支重置 `group.leader` | 删除该行 |
| `crates/rr-config/src/mem.rs:99-106` | `update_group_membership` 插入分支填充 leader(:103) | 构造时去掉 `leader` 键 |
| `crates/rr-config-etcd/src/lib.rs:400-407` | `get_routing_table` 从 replicas 派生 leader(:404) | 构造时去掉 `leader` 键 |
| `crates/rr-config-etcd/src/lib.rs:412-416` | 为无 group key 的 partition 补占位 `leader: None`(:414) | 构造时去掉 `leader` 键 |
| `crates/rr-router/src/router.rs:49-79` | `get_leader`(:55-65,:63 消费 `meta.leader`)与 `get_leader_addr`(:72-79,:77 含 metric) | **删除两个方法**及 `router.get_leader_addr.success` counter |
| `crates/rr-router/src/lib.rs:16-18, 34` | crate 文档与示例引用 `get_leader`/`get_leader_addr` | 文档改为只描述 `route`/`routing_table` |
| `crates/rr-core/src/error.rs:70-71` | `RouterError::NoLeader { group_id }` | 删除 variant(唯一生产者已删) |
| `crates/rr-network/src/error_convert.rs:146` | `NoLeader` → gRPC status 映射 | 删除映射分支 |

### 测试代码(必须修改)

| 位置 | 现状 | 改法 |
|---|---|---|
| `crates/rr-core/src/types.rs:74-82` | `test_group_meta` 构造 `leader: Some(1)` | 去掉 `leader` 键,断言改为 replicas/group_id |
| `crates/rr-config/src/mem.rs:170` | `assert_eq!(group.leader, Some(1))` | 删除断言 |
| `crates/rr-config/src/mem.rs:396` | 同上 | 删除断言 |
| `crates/rr-router/src/router.rs:140-167` | `test_get_leader_group_not_found` / `test_get_leader_success` | 随方法删除 |
| `crates/rr-router/src/router.rs:171-188, 225-256` | 3 个 `get_leader_addr_*` 测试 | 随方法删除 |
| `crates/rr-network/src/error_convert.rs:268` | `NoLeader` 转换测试 | 删除用例 |

### 确认不消费该字段、无需改动的相邻代码(证据)

- `crates/rr-server/src/dispatch.rs:54/142/157` —— 只用 `shard_router.route()`,redirect
  数据来自 openraft/openraft 错误,不经 `GroupMeta`;
- `crates/rr-server/src/lookup.rs` —— raft RPC 分发,与 routing table 无关;
- `crates/rr-server/src/node.rs:251-252` —— 只读 `meta.replicas` 决定是否 `start_group`;
- `crates/rr-server/src/admin.rs:157, 181` —— `leader_node_id` 来自 raft
  `metrics.current_leader`,是真实运行时 leader;
- `crates/rr-client/src/lib.rs` —— 不依赖 `rr-router`,leader 发现全部走
  `leader_cache` + `leader_info` redirect;
- `sdks/java/.../client/LeaderCache.java` —— 同构,只消费 proto `LeaderInfo`;
- `tests/integration/src/lib.rs:330/381` —— `wait_for_leader` / `wait_for_new_leader`
  基于 `metrics.current_leader`,与 `GroupMeta.leader` 无关;全 tests/ 目录无任何
  `GroupMeta` / `.leader` / `get_leader` 引用;
- `examples/` —— 仅注释中出现 "leader" 字样,无代码依赖。

### 文档(实施时同步,非本 ADR 范围)

`docs/docs/reference/config-reference.md:80/90`、`docs/docs/guide/config-provider.md:68/76`、
`docs/docs/architecture/c4/dynamic-shard-routing.md:36/62`、
`docs/docs/architecture/c4/components-router.md:62`、
`docs/docs/architecture/multi-raft.md:98/107`(均含 `GroupMeta.leader` 描述);
`docs/docs/reference/error-reference.md:68`(`NoLeader` 条目);
`docs/docs/reference/metrics-reference.md:300-314` 及 `:403` 的 PromQL 示例
(`router.get_leader_addr.success`)。`crates/rr-client/README.md:105` 引用的是 proto
`GroupInfo.leader_node_id`,不受影响。

## 测试方案

1. **单元测试收口**:上表"测试代码"逐项修改后,`cargo test -p rr-core -p rr-config
   -p rr-config-etcd -p rr-router -p rr-network` 全绿;rr-config-etcd 现有
   wire-shape 测试(`lib.rs:457-487`)不变,证明 etcd value 编码未动。
2. **新增:leader 切换后 client 无 `GroupMeta.leader` 仍正确重定向**(WS-7 验收核心)。
   集成测试,3 节点集群:
   - client 写入 group → 经 redirect 建立 leader cache;
   - `kill_node(当前 leader)`,用现有 `wait_for_new_leader`
     (`tests/integration/src/lib.rs:381`)等新 leader 选出;
   - client 再次写入,断言:首次尝试命中失效缓存/旧 leader 失败后,经 bootstrap
     轮询 + `leader_info` redirect 收敛到新 leader 并成功返回;
   - 该测试不构造、不断言任何 `GroupMeta.leader`。
   与现有 `test_ingestor_survives_leader_switch_no_message_loss`
   (`tests/integration/src/cross_tier_ingestor.rs:286`)的故障注入模式对齐。
3. **routing 更新回归**:`tests/integration` 中 routing/partition 变更相关用例全绿
   (WS-7 验收项);已核实该目录当前无任何 `GroupMeta.leader` 依赖,预期无需改测试
   代码,只需确认行为不变。
4. **etcd 升级演练(RC 期)**:真实 etcd 集群,0.1.x 写入的 key 数据由新代码
   `get_routing_table` 读取,断言 table 构建成功且 groups 内容一致(印证"存量数据
   零迁移"结论)。
5. **(可选)serde 防线**:为 `RoutingTable` 增加 JSON roundtrip 测试,锁定新 shape
   不含 `leader` 键;并验证含 `"leader":1` 的旧 JSON 能被新 struct 忽略字段反序列化,
   把"serde 忽略未知字段"的兜底行为固化为测试。

## Breaking 判定与下游迁移指引

**这是 breaking change**,必须在 v0.2.0 API freeze(WS-3)前落地,freeze 后不可再做。
破坏面全部为**编译期可发现**,无静默行为变更:

| 下游用法 | 升级后 | 迁移指引 |
|---|---|---|
| 构造 `GroupMeta { group_id, leader, replicas }` | 编译错误:unknown field `leader` | 删除 `leader` 键 |
| 读取 `group_meta.leader` | 编译错误:no field `leader` | 改用 client SDK:直接向集群发请求,按 `NotLeader`/`leader_info` redirect 收敛;不要用路由表猜 leader |
| 调用 `ShardRouter::get_leader` / `get_leader_addr` | 编译错误:no such method | 同上;若只需 group 放置信息,用 `route()` + `routing_table()` 读 `replicas` |
| `match` 中处理 `RouterError::NoLeader` | 编译错误:no such variant | 删除该分支;请求打到非 leader 时错误来自 server 端 redirect 机制 |
| 依赖 `router.get_leader_addr.success` metric 的看板/告警 | metric 消失 | 删除引用;leader 视角的可用性监控改用 raft metrics 暴露的 `is_leader`/`current_leader`(admin API 同源) |

对 SDK 用户(Rust `RrClient` / Java `RrClient`):**无迁移负担**,leader redirect
缓存机制不变,本次变更对其透明。运维侧:etcd 数据零迁移、proto 零变更、滚动升级
安全(见上文兼容分析)。

## Consequences

### Positive

- 单一事实源恢复:leader 只有一个权威——raft 运行时;routing table 回归纯配置
  语义,永不 stale;
- client 侧 leader 发现的正确性不再依赖任何静态猜测,redirect 数据来自 openraft
  `ForwardToLeader` 的实时视图;
- 公共 API 收缩(`rr-core` 类型、`rr-router` 方法、错误 variant、metric 各减一),
  freeze 后的演进负担降低;
- 与 A-0009 follow-up 引入的 `leader_addr` 形成完整闭环:wire 上早已备好替代机制,
  本 ADR 只是删掉被它取代的旧抽象。

### Negative

- breaking change,下游需要按上表迁移(编译期报错引导,风险可控);
- `ShardRouter` 不再提供"查 leader"能力——调用方必须改为"发请求 + 处理 redirect"
  的模型,对极少数离线诊断场景(只想查不想发请求)略不便,可用 admin API 的
  `leader_node_id`(真实值)替代;
- `get_cluster_info` 的 `leader_node_id: 0` 占位问题被本 ADR 显式搁置(留待后续
  从 raft metrics 填充),短期内该字段仍无信息价值。

### Neutral

- etcd schema(A-0004)key layout 与 value 编码不变,无需版本演进;
- 集成测试目录零改动(已核实无依赖),改动集中在 4 个 crate 的单元测试;
- `GroupMeta` 序列化 shape 变化(少一个键)无实际影响:全仓无其序列化消费者。

## References

- `.sisyphus/evidence/rr-full-review-2026-08-16.md` —— 设计错误 #5 / P0#8 / Oracle Q3
- `.sisyphus/plans/rr-v0.2.0-release-roadmap.md` —— WS-7(本 ADR 的实施工作项)
- `.sisyphus/adr/A-0004-etcd-config-schema.md` —— etcd key schema(结论:不受本变更影响)
- `.sisyphus/adr/A-0009-read-write-separation-and-sm-locking.md` —— 2026-05-13 changelog:
  `leader_addr` 与 `ReadLocalResponse.leader_info` redirect 机制
- `crates/rr-core/src/types.rs`、`crates/rr-config/src/mem.rs`、
  `crates/rr-config-etcd/src/lib.rs`、`crates/rr-router/src/router.rs`、
  `crates/rr-client/src/lib.rs`、`crates/rr-server/src/dispatch.rs`

## Changelog

| Date | Change |
|------|--------|
| 2026-08-28 | Initial: 删除 `GroupMeta.leader` 的决策;替代机制(client redirect 缓存)论证;etcd/wire 零迁移的核实结论;全部受影响调用点清单;测试方案与下游迁移指引 |
| 2026-08-29 | Implemented(WS-7):按 Option A 落地。`GroupMeta.leader` 字段、`ShardRouter::get_leader`/`get_leader_addr`、`RouterError::NoLeader`(含 gRPC 映射分支)与 `router.get_leader_addr.success` metric 全部删除,"受影响调用点清单"逐项收口;`GroupMeta` 补 rustdoc 声明纯配置面语义。新增 serde 防线测试:新 shape 无 `leader` 键 + 旧含 `"leader"` 键 JSON 被静默忽略(`crates/rr-core/src/types.rs:91`/`:103`);新增 `tests/integration/src/leader_redirect_failover.rs:26` `test_client_redirect_converges_after_leader_kill` e2e(kill leader 后 client 经 bootstrap 轮询 + `leader_info` redirect 收敛到新 leader)。`crates/rr-server/src/dispatch.rs:193-198` 的 `leader_node_id: 0` 占位加约束注释:真实值只能取自 raft `metrics.current_leader`,不得恢复路由表字段。 |

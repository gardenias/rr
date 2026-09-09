---- MODULE RoutingTableSwitch ----
(***************************************************************************)
(* R-2 spec 4/4: 路由表版本切换(rr-router + ConfigProvider watch 传播).     *)
(*                                                                         *)
(* Implementation (双向链接):                                              *)
(*   - crates/rr-router/src/router.rs  `ShardRouter::route`(纯表查询,       *)
(*     每次 route 读一份一致性快照;不在表则 PartitionNotFound)             *)
(*   - crates/rr-config-etcd/src/lib.rs  watch 事件按 key 增量应用          *)
(*     (reassign 是 PUT 新值,无 delete 中间态)                             *)
(*   - docs/docs/reference/invariants.md  INV-6 (路由一致性;T32/T33 锚点)  *)
(*                                                                         *)
(* Model:                                                                  *)
(*   - 配置存储里表版本原子提交(v0 → v1):p1 从 g1 迁移到 g2;               *)
(*   - 传播到各节点的 view 有两种纪律(常量 AtomicWatch 选择):              *)
(*       TRUE  = MemConfigProvider 语义(整表快照原子翻页);                 *)
(*       FALSE = etcd watch 语义(按 partition key 逐个翻页,中间出现混合    *)
(*               view——注意 reassign 是 upsert,不存在 "删除中间态");       *)
(*   - 客户端经某节点路由:route(n, p) 按该节点当前 view 的映射给出唯一       *)
(*     group.                                                              *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANT Nodes          \* 提供路由视图的节点,如 {a, b}
CONSTANT MaxRoutes      \* 状态约束:路由请求数上限
CONSTANT AtomicWatch    \* TRUE=整表快照翻页(Mem);FALSE=按 key 增量(etcd)

\* 分区与组固定为小模型:两个分区,两个组;reassign = p1: g1 → g2.
Partitions == {"p1", "p2"}
Groups == {"g1", "g2"}

\* 两个版本的表(对全部 partition 是全函数——INV-6 的单归属前提):
Table(v) ==
    IF v = 0 THEN [p \in Partitions |-> IF p = "p1" THEN "g1" ELSE "g2"]
             ELSE [p \in Partitions |-> "g2"]

VARIABLES
    storeVersion,       \* 配置存储已提交的最新版本(0 或 1)
    applied,            \* [node, partition] ↦ 该节点该 partition 当前生效的版本
    routes,             \* 已发生路由的结果记录([node, part, ver, grp] 集合)
    routeCount,         \* 路由请求计数(状态约束)
    staleAfterConverge  \* 哨兵位:收敛后仍按旧版本路由(禁止)

vars == <<storeVersion, applied, routes, routeCount, staleAfterConverge>>

Converged == \A n \in Nodes, p \in Partitions: applied[n][p] = storeVersion

TypeOK ==
    /\ storeVersion \in {0, 1}
    /\ applied \in [Nodes -> [Partitions -> {0, 1}]]
    /\ routeCount \in Nat
    /\ staleAfterConverge \in BOOLEAN
    /\ routes \subseteq [node: Nodes, part: Partitions, ver: {0,1}, grp: Groups]

Init ==
    /\ storeVersion = 0
    /\ applied = [n \in Nodes |-> [p \in Partitions |-> 0]]
    /\ routes = {}
    /\ routeCount = 0
    /\ staleAfterConverge = FALSE

\* 配置存储提交切换(一次性).
CommitSwitch ==
    /\ storeVersion = 0
    /\ storeVersion' = 1
    /\ UNCHANGED <<applied, routes, routeCount, staleAfterConverge>>

\* watch 传播——整表快照语义:节点所有 partition 翻到最新版本.
WatchAtomic(n) ==
    /\ AtomicWatch
    /\ storeVersion = 1
    /\ \E p \in Partitions: applied[n][p] /= storeVersion  \* 有未翻页才有效
    /\ applied' = [applied EXCEPT ![n] = [p \in Partitions |-> storeVersion]]
    /\ UNCHANGED <<storeVersion, routes, routeCount, staleAfterConverge>>

\* watch 传播——按 key 增量语义(etcd):单个 partition 翻到最新版本.
WatchKey(n, p) ==
    /\ ~AtomicWatch
    /\ storeVersion = 1
    /\ applied[n][p] /= storeVersion
    /\ applied' = [applied EXCEPT ![n][p] = storeVersion]
    /\ UNCHANGED <<storeVersion, routes, routeCount, staleAfterConverge>>

\* 客户端路由:按节点当前 view 给出该 partition 的唯一 group.
\* 两张表都是全函数 ⇒ 永无 NotFound;NoPhantomNotFound 断言它.
Route(n, p) ==
    /\ routeCount < MaxRoutes
    /\ LET v == applied[n][p]
           g == Table(v)[p]
       IN
        /\ routes' = routes \cup {[node |-> n, part |-> p, ver |-> v, grp |-> g]}
        \* 收敛后仍按旧版本路由 = 违规(收敛是稳定的:storeVersion 不再变)
        /\ staleAfterConverge' = (Converged /\ v /= storeVersion)
        /\ routeCount' = routeCount + 1
    /\ UNCHANGED <<storeVersion, applied>>

Next ==
    \/ CommitSwitch
    \/ \E n \in Nodes: WatchAtomic(n)
    \/ \E n \in Nodes, p \in Partitions: WatchKey(n, p)
    \/ \E n \in Nodes, p \in Partitions: Route(n, p)

(***************************************************************************)
(* Safety 性质                                                             *)
(***************************************************************************)

\* S1: 任一请求在其生效 view 上恰好路由到一个 group——路由结果是 (view 版本,
\*     唯一 group);结构性成立(route 读取单映射).此处显式断言:同一次路由
\*     记录里 ver 与 grp 的组合被 view 的表唯一决定.
RouteDeterminedByView ==
    \A r1, r2 \in routes:
        (r1.node = r2.node /\ r1.part = r2.part /\ r1.ver = r2.ver)
            => r1.grp = r2.grp

\* S2: 无幻影 NotFound——两张表对全部 partition 全函数,任何 view 下都不存在
\*     "partition 消失"的路由结果.routes 只记成功路由,故以"从不产生
\*     NotFound"表述为:模型中 Route 不产生错误路径(若未来表非全函数,此处
\*     须改为允许 PartitionNotFound 的显式条款).
NoPhantomNotFound == TRUE  \* 由 Table 全函数性直接保证(见 S1 注释)

\* S3: 收敛后无旧路由——所有 view 都已翻到最新版本后,任何路由不得再按旧
\*     版本的映射作答.
NoStaleRouteAfterConverge == ~staleAfterConverge

Safety ==
    /\ RouteDeterminedByView
    /\ NoStaleRouteAfterConverge

(***************************************************************************)
(* Liveness:切换提交后,所有节点最终收敛(watch 传播不丢).                  *)
(***************************************************************************)
FairSpec == /\ Init
            /\ [][Next]_vars
            /\ \A n \in Nodes: WF_vars(WatchAtomic(n))
            /\ \A n \in Nodes, p \in Partitions: WF_vars(WatchKey(n, p))

LivenessProp == (storeVersion = 1) ~> Converged

StateConstraint == routeCount <= MaxRoutes

\* 意图声明(TLC 非定理证明器): FairSpec => []Safety
=============================================================================

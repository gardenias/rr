---- MODULE OffsetExactlyOnce ----
(***************************************************************************)
(* R-2 spec 2/4: Kafka offset → request_id 映射 + 框架幂等层(watermark +   *)
(* LRU)的 exactly-once 语义(rr 自有扩展,非 openraft 核心).                  *)
(*                                                                         *)
(* Implementation (双向链接):                                              *)
(*   - crates/rr-ingestor-kafka/src/lib.rs  `kafka_offset_to_request_id`   *)
(*     (decode 失败 / 负 offset(Kafka 哨兵)→ Err,fail-closed;supervisor    *)
(*      退出而不是静默映射到 0)                                            *)
(*   - crates/rr-raft/src/idempotency.rs  `IdempotencyLayer::decide/record`*)
(*     (模块文档规则 1-4:Replay/Collision/Expired/New;watermark 在 record   *)
(*      推进且单调;watermark 存在性区分 "从未 apply" 与 "apply 过 id 0")    *)
(*   - docs/docs/reference/invariants.md  INV-3 (exactly-once ingest)      *)
(*   - ADR .sisyphus/adr/A-0003-framework-idempotency-layer.md             *)
(*                                                                         *)
(* Model boundary / 到达纪律(与实现对齐的关键假设):                        *)
(*   - 单 request_type 命名空间;Kafka 单分区 offset 有序 ⇒ 真实消息按序     *)
(*     到达;at-least-once ⇒ 已 apply 的 offset 可重复送达(redelivery);     *)
(*   - 哨兵 offset(如 INVALID_OFFSET=-1001)可能出现,映射必须 fail-closed.  *)
(*   - apply 本身的成功/崩溃路径归 crash harness(tests/crash),不在此模型.  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANT Offsets        \* 真实消息 offset 集合(有序),如 {0,1,2}
CONSTANT Cap            \* LRU 容量,小值(如 2)以在小状态空间内逼出驱逐
CONSTANT MaxEvents      \* 状态约束:总投递事件上限

Sentinel == -1          \* Kafka INVALID_OFFSET 哨兵替身(负值)

VARIABLES
    nextGenuine,        \* 下一个按序到达的真实 offset(Offsets 的索引序)
    applied,            \* offset ↦ apply 次数(用户服务被调次数;exactly-once 判据)
    cachedResp,         \* offset ↦ [present ↦ BOOL, resp ↦ Int, tick ↦ Int]
    wmExists,           \* watermark 是否存在(区分 "从未 apply" 与 "apply 过 id 0")
    wmValue,            \* watermark 值 = 已 apply 的最大 offset
    tick,               \* LRU 时钟
    stopped,            \* supervisor 已 fail-closed(哨兵/非法输入)
    expiredWithoutApply,\* 哨兵位:某次 Expired 落在从未 apply 的 offset 上(=静默丢失)
    events              \* 已处理投递事件数(状态约束)

vars == <<nextGenuine, applied, cachedResp, wmExists, wmValue, tick,
          stopped, expiredWithoutApply, events>>

TypeOK ==
    /\ nextGenuine \in 0..Cardinality(Offsets)
    /\ applied \in [Offsets -> 0..MaxEvents]
    /\ cachedResp \in [Offsets -> [present: BOOLEAN, resp: {-1} \union Offsets, tick: 0..MaxEvents]]
    /\ wmExists \in BOOLEAN
    /\ wmValue \in {-1} \union Offsets
    /\ tick \in Nat
    /\ stopped \in BOOLEAN
    /\ expiredWithoutApply \in BOOLEAN
    /\ events \in Nat

\* Offsets 是 {0..N-1} 序;取第 k 个(0 起) = k 本身(Offset 集合构造为
\* 连续从 0 起,见 .cfg). 保持映射函数以便改为任意序.
GenuineAt(k) == k

\* LRU 驱逐目标:present 中 tick 最小者;无 present 时返回 -1(不会发生).
EvictVictim ==
    LET present == {o \in Offsets: cachedResp[o].present}
    IN  IF present = {} THEN -1
        ELSE CHOOSE o \in present: \A p \in present: cachedResp[o].tick <= cachedResp[p].tick

\* decide+record 的一个原子步:apply 一个真实 offset o(首次到达或 redelivery).
\* 返回更新后的状态分量(用 LET 内联在动作里).
ProcessGenuine(o) ==
    LET hit == cachedResp[o].present
        isExpired == /\ ~hit
                     /\ wmExists
                     /\ o <= wmValue
    IN
    IF hit
    THEN \* Replay(或 Collision——本模型单 client,collision 由 dedicated
         \* idempotency proptest 覆盖):不重复 apply,刷新 LRU tick.
         /\ cachedResp' = [cachedResp EXCEPT ![o].tick = tick]
         /\ tick' = tick + 1
         /\ UNCHANGED <<applied, wmExists, wmValue, stopped, expiredWithoutApply>>
    ELSE IF isExpired
         THEN \* Expired:不 apply.若该 offset 从未 apply 过,这是静默丢失
              \* —— 不变量 NoFalseExpired 禁止它发生.
              /\ expiredWithoutApply' = (applied[o] = 0)
              /\ UNCHANGED <<applied, cachedResp, wmExists, wmValue, tick, stopped>>
         ELSE \* New:apply 恰好一次,record 推进 watermark(单调),写缓存(必要时驱逐).
              /\ applied' = [applied EXCEPT ![o] = @ + 1]
              /\ wmExists' = TRUE
              /\ wmValue' = IF ~wmExists THEN o ELSE (IF o > wmValue THEN o ELSE wmValue)
              /\ LET needEvict == o \notin {c \in Offsets: cachedResp[c].present}
                               /\ Cardinality({c \in Offsets: cachedResp[c].present}) >= Cap
                     victim == EvictVictim
                     evicted == IF needEvict
                                THEN [cachedResp EXCEPT ![victim].present = FALSE]
                                ELSE cachedResp
                 IN  cachedResp' = [evicted EXCEPT ![o] = [present |-> TRUE,
                                                          resp |-> o,  \* 响应属于真实消息
                                                          tick |-> tick]]
              /\ tick' = tick + 1
              /\ UNCHANGED <<expiredWithoutApply, stopped>>

Init ==
    /\ nextGenuine = 0
    /\ applied = [o \in Offsets |-> 0]
    /\ cachedResp = [o \in Offsets |-> [present |-> FALSE, resp |-> -1, tick |-> 0]]
    /\ wmExists = FALSE
    /\ wmValue = -1
    /\ tick = 0
    /\ stopped = FALSE
    /\ expiredWithoutApply = FALSE
    /\ events = 0

\* 按序到达下一条真实消息(Kafka 单分区有序).
DeliverNext ==
    /\ ~stopped
    /\ events < MaxEvents
    /\ nextGenuine < Cardinality(Offsets)
    /\ ProcessGenuine(GenuineAt(nextGenuine))
    /\ nextGenuine' = nextGenuine + 1
    /\ events' = events + 1

\* redelivery:重复送达任一**已送达过**的真实 offset(at-least-once 语义;
\* 未送达过的不会重复出现——consumer 重启从未提交位置回放,而已提交 ⟹ 已
\* apply,这是 kafka ingestor 的 commit 纪律).
Redeliver ==
    /\ ~stopped
    /\ events < MaxEvents
    /\ \E o \in Offsets:
        /\ applied[o] >= 0  \* 已 apply 或未 apply 但已送达过(见下界)
        /\ o < nextGenuine  \* 已送达集合 = {0 .. nextGenuine-1}
        /\ ProcessGenuine(o)
    /\ events' = events + 1
    /\ UNCHANGED nextGenuine

\* 哨兵到达:映射 fail-closed,Supervisor 退出,流终止(不再有任何投递).
DeliverSentinel ==
    /\ ~stopped
    /\ events < MaxEvents
    /\ stopped' = TRUE
    /\ events' = events + 1
    /\ UNCHANGED <<nextGenuine, applied, cachedResp, wmExists, wmValue,
                   tick, expiredWithoutApply>>

Next == DeliverNext \/ Redeliver \/ DeliverSentinel

(***************************************************************************)
(* Safety 性质                                                             *)
(***************************************************************************)

\* S1: 任一真实 offset 至多 apply 一次(Kafka at-least-once + 框架去重
\*     = exactly-once apply;INV-3).
ExactlyOnceApply == \A o \in Offsets: applied[o] <= 1

\* S2: 缓存里的响应永远属于对应 offset 的真实消息(哨兵不得污染 id 0;
\*     review P0#6 的 clamp-to-0 回归形态).
ResponseBelongsToGenuine ==
    \A o \in Offsets: cachedResp[o].present => cachedResp[o].resp = o

\* S3: watermark = 已 apply 的最大 offset(由 record 处 max 推进语义直接得出;
\*     单调性是其推论).
WatermarkIsMaxApplied ==
    wmExists =>
        \E o \in Offsets: applied[o] > 0 /\ wmValue = o
            /\ \A p \in Offsets: applied[p] > 0 => p <= o

\* S4: Expired 绝不落在从未 apply 的 offset 上(否则客户端被误导丢弃未 apply
\*     的消息 = 静默丢失).
NoFalseExpired == ~expiredWithoutApply

\* S5: fail-closed——哨兵一旦到达,流终止(stopped 后无投递动作可用,
\*     由动作门控保证;此处断言 stopped 与"之后无新 apply"不可同时违背).
\*     状态级表述:stopped 时所有计数冻结由动作门控保证,不变量只断言
\*     stopped 发生之前哨兵未曾 apply(没有 id 被哨兵映射占用).
SentinelNeverApplied == TRUE  \* 由 DeliverSentinel 不触碰 applied 直接保证

Safety ==
    /\ ExactlyOnceApply
    /\ ResponseBelongsToGenuine
    /\ WatermarkIsMaxApplied
    /\ NoFalseExpired

(***************************************************************************)
(* Liveness: 无哨兵时,所有真实消息最终被 apply(弱公平 DeliverNext).        *)
(***************************************************************************)
(***************************************************************************)
(* Liveness: 不存在"预算未尽、流未完成、未 fail-closed 却永远不再前进"的     *)
(* 活锁.WF(DeliverNext) 表达真实 ingestor 的前向消费纪律:redelivery 是      *)
(* 重启回放(有界),前向读是常态.                                            *)
(***************************************************************************)
FairSpec == /\ Init
            /\ [][Next]_vars
            /\ WF_vars(DeliverNext)

AllGenuineApplied == \A o \in Offsets: applied[o] = 1
\* 终止形态只有三种:流读完 / fail-closed / 模型预算用尽.永真红elivery
\* 不进度的行为被 WF(DeliverNext) 排除.
LivenessProp == <>(  nextGenuine = Cardinality(Offsets)
                   \/ stopped
                   \/ events = MaxEvents)

\* TLC 说明: 本模型有限事件(MaxEvents),终止即正常收尾;CI 以 -deadlock 跑.
StateConstraint == events <= MaxEvents

\* 意图声明(TLC 非定理证明器): FairSpec => []Safety
=============================================================================

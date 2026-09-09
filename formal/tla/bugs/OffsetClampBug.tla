---- MODULE OffsetClampBug ----
(***************************************************************************)
(* 故意植入反例(实证用,不进 CI 绿集):负 offset 哨兵 clamp 到 0(旧实现     *)
(* `i.max(0)` 形态,review P0#6 的原始缺陷)——哨兵被映射到 request_id 0 并  *)
(* 正常 apply,污染真实首条消息的幂等身份.                                  *)
(*                                                                         *)
(* TLC 预期抓到(两种轨迹任一):                                           *)
(*   - ResponseBelongsToGenuine 违反(哨兵先于真实 offset 0 到达,id 0 的    *)
(*     缓存响应被哨兵占用);或                                              *)
(*   - ExactlyOnceApply 违反(哨兵在真实 offset 0 之后到达,id 0 被 apply    *)
(*     两次).                                                              *)
(* 实证记录见 formal/tla/README.md "植入反例实证" 一节.                    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANT Offsets
CONSTANT Cap
CONSTANT MaxEvents

VARIABLES
    nextGenuine, applied, cachedResp, wmExists, wmValue, tick, events

vars == <<nextGenuine, applied, cachedResp, wmExists, wmValue, tick, events>>

EvictVictim ==
    LET present == {o \in Offsets: cachedResp[o].present}
    IN  IF present = {} THEN -1
        ELSE CHOOSE o \in present: \A p \in present: cachedResp[o].tick <= cachedResp[p].tick

\* BUG: 哨兵/负 offset 被 clamp 到 0 并正常进入 decide/apply 流程.
\* resp 用 -1 标记"哨兵的内容",以便不变量识别污染.
ProcessGenuineWithResp(o, resp) ==
    LET hit == cachedResp[o].present
        isExpired == /\ ~hit /\ wmExists /\ o <= wmValue
    IN
    IF hit
    THEN /\ cachedResp' = [cachedResp EXCEPT ![o].tick = tick]
         /\ tick' = tick + 1
         /\ UNCHANGED <<applied, wmExists, wmValue>>
    ELSE IF isExpired
         THEN UNCHANGED <<applied, cachedResp, wmExists, wmValue, tick>>
         ELSE /\ applied' = [applied EXCEPT ![o] = @ + 1]
              /\ wmExists' = TRUE
              /\ wmValue' = IF ~wmExists THEN o ELSE (IF o > wmValue THEN o ELSE wmValue)
              /\ LET needEvict == o \notin {c \in Offsets: cachedResp[c].present}
                               /\ Cardinality({c \in Offsets: cachedResp[c].present}) >= Cap
                     victim == EvictVictim
                     evicted == IF needEvict
                                THEN [cachedResp EXCEPT ![victim].present = FALSE]
                                ELSE cachedResp
                 IN  cachedResp' = [evicted EXCEPT ![o] = [present |-> TRUE,
                                                          resp |-> resp,
                                                          tick |-> tick]]
              /\ tick' = tick + 1

TypeOK ==
    /\ nextGenuine \in 0..Cardinality(Offsets)
    /\ applied \in [Offsets -> 0..MaxEvents]
    /\ cachedResp \in [Offsets -> [present: BOOLEAN, resp: {-1} \union Offsets, tick: 0..MaxEvents]]
    /\ wmExists \in BOOLEAN
    /\ wmValue \in {-1} \union Offsets
    /\ tick \in Nat
    /\ events \in Nat

Init ==
    /\ nextGenuine = 0
    /\ applied = [o \in Offsets |-> 0]
    /\ cachedResp = [o \in Offsets |-> [present |-> FALSE, resp |-> -1, tick |-> 0]]
    /\ wmExists = FALSE
    /\ wmValue = -1
    /\ tick = 0
    /\ events = 0

DeliverNext ==
    /\ events < MaxEvents
    /\ nextGenuine < Cardinality(Offsets)
    /\ ProcessGenuineWithResp(nextGenuine, nextGenuine)  \* 真实消息:resp = 自身 offset
    /\ nextGenuine' = nextGenuine + 1
    /\ events' = events + 1

Redeliver ==
    /\ events < MaxEvents
    /\ \E o \in Offsets:
        /\ o < nextGenuine
        /\ ProcessGenuineWithResp(o, o)
    /\ events' = events + 1
    /\ UNCHANGED nextGenuine

\* BUG 路径:哨兵到达,clamp 到 0,正常 apply(resp = -1 哨兵标记).
DeliverSentinelClamped ==
    /\ events < MaxEvents
    /\ ProcessGenuineWithResp(0, -1)
    /\ events' = events + 1
    /\ UNCHANGED nextGenuine

Next == DeliverNext \/ Redeliver \/ DeliverSentinelClamped

ExactlyOnceApply == \A o \in Offsets: applied[o] <= 1
ResponseBelongsToGenuine ==
    \A o \in Offsets: cachedResp[o].present => cachedResp[o].resp = o

Safety == /\ ExactlyOnceApply /\ ResponseBelongsToGenuine

FairSpec == /\ Init /\ [][Next]_vars /\ WF_vars(DeliverNext)
StateConstraint == events <= MaxEvents

=============================================================================

---- MODULE OutputWatermark ----
(***************************************************************************)
(* R-2 spec 3/4: output/outbox watermark(A-0007,rr 自有扩展).               *)
(*                                                                         *)
(* Implementation (双向链接):                                              *)
(*   - crates/rr-raft/src/service_adapter.rs  `append_outputs`(seq 单调分配  *)
(*     入 pending)/ `SharedOutboxControl::pending_outputs`(seq > watermark)*)
(*     / `advance_watermark`(max 推进 + retain 清除已确认)                 *)
(*   - crates/rr-raft/src/output_runner.rs  发送循环:leader-only、按序遍历  *)
(*     pending、成功才 advance、失败指数退避并 break(下一轮重试)           *)
(*   - docs/docs/reference/invariants.md  INV-4 (output 至少一次送达)       *)
(*   - ADR .sisyphus/adr/A-0007-output-outbox-mechanism.md                 *)
(*                                                                         *)
(* Model boundary:                                                         *)
(*   - 发送目标(RaftWrite/KafkaPublish)的对端语义不建模;成功/失败由非确定   *)
(*     性给出;                                                              *)
(*   - 关键交错:leader 在 "发送成功、watermark 未推进" 窗口内切换 ⇒ 新       *)
(*     leader 从 watermark+1 重发 ⇒ 重复投递(at-least-once 承诺语义,spec   *)
(*     显式允许重复、禁止静默丢失).                                        *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANT MaxSeq         \* 产生的 output 数上限(如 4)
CONSTANT MaxFailovers   \* leader 切换次数上限(如 2)
CONSTANT MaxSteps       \* 状态约束:总步数上限

VARIABLES
    nextSeq,            \* 已分配的最大 seq(= 已产生的 output 数;seq 从 1 起)
    watermark,          \* 已确认送达的最大 seq
    deliveries,         \* seq ↦ 投递次数(允许 >1:at-least-once)
    inflight,           \* 当前 runner 已发送但尚未 advance 的 seq(0 = 无)
    runnerAlive,        \* 当前 runner 是否在任(leader-only 执行)
    failovers,          \* 已发生的切换次数
    steps               \* 已执行步数(状态约束)

vars == <<nextSeq, watermark, deliveries, inflight, runnerAlive, failovers, steps>>

TypeOK ==
    /\ nextSeq \in 0..MaxSeq
    /\ watermark \in 0..MaxSeq
    /\ deliveries \in [1..MaxSeq -> 0..MaxSteps]
    /\ inflight \in 0..MaxSeq
    /\ runnerAlive \in BOOLEAN
    /\ failovers \in 0..MaxFailovers
    /\ steps \in 0..MaxSteps

Init ==
    /\ nextSeq = 0
    /\ watermark = 0
    /\ deliveries = [s \in 1..MaxSeq |-> 0]
    /\ inflight = 0
    /\ runnerAlive = TRUE
    /\ failovers = 0
    /\ steps = 0

\* apply 路径产生一个 output(append_outputs: seq = ++next_seq).
Produce ==
    /\ steps < MaxSteps
    /\ nextSeq < MaxSeq
    /\ nextSeq' = nextSeq + 1
    /\ steps' = steps + 1
    /\ UNCHANGED <<watermark, deliveries, inflight, runnerAlive, failovers>>

\* runner 按序取下一个 pending(seq = watermark+1)开始发送.
\* (output_runner: pending 按 seq 有序遍历,失败即 break.)
SendNext ==
    /\ steps < MaxSteps
    /\ runnerAlive
    /\ inflight = 0
    /\ watermark < nextSeq
    /\ inflight' = watermark + 1
    /\ steps' = steps + 1
    /\ UNCHANGED <<nextSeq, watermark, deliveries, runnerAlive, failovers>>

\* 发送成功(dispatch_one Ok):投递计数 +1;advance 尚未发生——
\* 两者之间的窗口就是切换导致重复的来源.
SendOk ==
    /\ steps < MaxSteps
    /\ runnerAlive
    /\ inflight /= 0
    /\ deliveries' = [deliveries EXCEPT ![inflight] = @ + 1]
    /\ steps' = steps + 1
    /\ UNCHANGED <<nextSeq, watermark, runnerAlive, failovers>>
    /\ UNCHANGED inflight   \* 等待 Advance

\* 发送成功后的 advance_watermark(seq):max 推进(保序)+ retain 清除.
Advance ==
    /\ steps < MaxSteps
    /\ runnerAlive
    /\ inflight /= 0
    /\ deliveries[inflight] > 0          \* 必须真的投递过才可确认
    /\ watermark' = inflight             \* inflight == watermark+1(保序)
    /\ inflight' = 0
    /\ steps' = steps + 1
    /\ UNCHANGED <<nextSeq, deliveries, runnerAlive, failovers>>

\* 发送失败:本轮 break,下轮重试(无状态变化,只耗步数预算).
SendFail ==
    /\ steps < MaxSteps
    /\ runnerAlive
    /\ inflight /= 0
    /\ inflight' = 0
    /\ steps' = steps + 1
    /\ UNCHANGED <<nextSeq, watermark, deliveries, runnerAlive, failovers>>

\* leader 切换:当前 runner 退出,未 advance 的 inflight 丢失(advance 是
\* raft 写,随旧 leader 而去);新 runner 从 watermark+1 重来 ⇒ 重复投递.
Failover ==
    /\ steps < MaxSteps
    /\ failovers < MaxFailovers
    /\ runnerAlive
    /\ runnerAlive' = FALSE   \* 本步退出…
    /\ inflight' = 0
    /\ failovers' = failovers + 1
    /\ steps' = steps + 1
    /\ UNCHANGED <<nextSeq, watermark, deliveries>>

\* 新 runner 上任(新 leader 的 on_leader_elected).
Respawn ==
    /\ steps < MaxSteps
    /\ ~runnerAlive
    /\ runnerAlive' = TRUE
    /\ steps' = steps + 1
    /\ UNCHANGED <<nextSeq, watermark, deliveries, inflight, failovers>>

Next == Produce \/ SendNext \/ SendOk \/ Advance \/ SendFail \/ Failover \/ Respawn

(***************************************************************************)
(* Safety 性质                                                             *)
(***************************************************************************)

\* S1: 无幻影推进——watermark 覆盖的每个 seq 都真实投递过(不允许静默丢失).
\*     "seq ≤ watermark ⟹ deliveries[seq] ≥ 1".
NoPhantomAdvance ==
    \A s \in 1..MaxSeq: s <= watermark => deliveries[s] >= 1

\* S2: 保序推进——inflight 永远是 watermark+1(发送按 pending 顺序),
\*     故 watermark 从不跳号.状态级表述:inflight ∈ {0, watermark+1}.
InOrderCursor == inflight = 0 \/ inflight = watermark + 1

\* S3: watermark 不超过已产生的最大 seq(不确认未产生的 output).
\*     (monotone 由 Advance 的结构直接保证:watermark 只被赋 inflight. )
WatermarkWithinProduced == watermark <= nextSeq

\* S4: inflight 不超过已产生.
InflightWithinProduced == inflight <= nextSeq

Safety ==
    /\ NoPhantomAdvance
    /\ InOrderCursor
    /\ WatermarkWithinProduced
    /\ InflightWithinProduced

(***************************************************************************)
(* Liveness:发送持续成功 ⟹ 所有已产生的 output 最终被确认送达(允许重复).   *)
(* 弱公平于 Advance/Respawn;SendFail/Failover 无公平约束(失败可有界发生).  *)
(* 注:"允许重复"是显式承诺:deliveries[s] 可以 > 1(Failover 在 SendOk 与    *)
(* Advance 之间发生时必发生)——这不是违规,违规只有"未投递却 advance".      *)
(***************************************************************************)
FairSpec == /\ Init
            /\ [][Next]_vars
            /\ WF_vars(Advance)
            /\ WF_vars(Respawn)

\* 终止形态:全部确认 / 预算用尽 / runner 已死且无人复活(被公平性排除的
\* 无限 Failover 不在此列——failovers 有界,最终必停在 runnerAlive).
LivenessProp == <>(  watermark = nextSeq
                   \/ steps = MaxSteps)

StateConstraint == steps <= MaxSteps

\* 意图声明(TLC 非定理证明器): FairSpec => []Safety
=============================================================================

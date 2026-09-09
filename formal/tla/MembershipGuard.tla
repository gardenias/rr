---- MODULE MembershipGuard ----
(***************************************************************************)
(* R-2 spec 1/4: membership-change quorum guard (rr 自有协议,非 openraft   *)
(* 核心).                                                                  *)
(*                                                                         *)
(* Implementation (双向链接):                                              *)
(*   - crates/rr-server/src/admin.rs  `AdminServiceImpl::change_membership`*)
(*     (guard: empty set rejected unconditionally → quorum guard target-only*)
(*      → force bypass with WARN audit; openraft full-replacement apply)   *)
(*   - crates/rr-core/src/membership.rs `MIN_VOTERS_AFTER_REMOVAL = 2`,    *)
(*     `membership_change_is_safe`                                         *)
(*   - docs/docs/reference/invariants.md  INV-7 (membership guard)         *)
(*   - ADR .sisyphus/adr/A-0012-membership-quorum-guard.md                 *)
(*                                                                         *)
(* Model boundary: openraft 的 joint consensus / 日志复制属上游已验证范围;  *)
(* 这里建模的是 rr 自己的 admin 入口策略——guard 的求值只依赖目标 voter 集    *)
(* 大小(full-replacement 语义使每个请求自含,无 TOCTOU 组合),raft 日志把并发  *)
(* 请求串行化(顺序任意).                                                   *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANT Nodes          \* 节点全集,如 {n1, n2, n3}
CONSTANT MinVoters      \* = 2 (rr_core::membership::MIN_VOTERS_AFTER_REMOVAL)
CONSTANT MaxRequests    \* 状态约束:提交请求总数上限(CI 用小值)

\* 一个 in-flight change_membership 请求: [target ∈ SUBSET Nodes, force ∈ BOOL]
VARIABLES
    committed,          \* 当前已提交的 voter 集合(SUBSET Nodes)
    inFlight,           \* 已受理待裁决的请求集合
    submitted,          \* 已提交过的请求计数(状态约束用)
    forcedCommitted,    \* 当前 committed 是否来自一次 force 通过(布尔)
    auditLog            \* force 通过的审计记录集合(SUBSET Nodes 的集合)

vars == <<committed, inFlight, submitted, forcedCommitted, auditLog>>

TypeOK ==
    /\ committed \subseteq Nodes
    /\ inFlight \subseteq [target: SUBSET Nodes, force: BOOLEAN]
    /\ submitted \in Nat
    /\ forcedCommitted \in BOOLEAN
    /\ auditLog \subseteq SUBSET Nodes

Init ==
    /\ committed = Nodes                    \* 集群以完整名单起步
    /\ inFlight = {}
    /\ submitted = 0
    /\ forcedCommitted = FALSE
    /\ auditLog = {}

(***************************************************************************)
(* 客户端提交一个变更请求.admin handler 的三个判断按实现顺序(admin.rs:      *)
(* existence → empty(unconditional)→ quorum guard(target-only, force 旁路  *)
(* 带审计)→ apply).                                                        *)
(***************************************************************************)
Submit ==
    /\ submitted < MaxRequests
    /\ \E t \in SUBSET Nodes, f \in {TRUE, FALSE} :
        \* 任意目标(含空集——guard 必须拒绝它,模型要覆盖这条路径)
        /\ inFlight' = inFlight \cup {[target |-> t, force |-> f]}
        /\ submitted' = submitted + 1
    /\ UNCHANGED <<committed, forcedCommitted, auditLog>>

\* 裁决一个 in-flight 请求(raft 串行化 ⇒ 任选一个即所有交错).
\* 返回三种结局: reject-empty(无条件) / reject-unsafe / commit(含 force).
Resolve ==
    \E r \in inFlight:
        LET t == r.target
            f == r.force
        IN
        /\ inFlight' = inFlight \ {r}
        /\ IF t = {}
           THEN \* n→0 是拆除,force 不可绕过(admin.rs:593-609)
                /\ UNCHANGED <<committed, submitted, forcedCommitted, auditLog>>
           ELSE IF Cardinality(t) >= MinVoters
                THEN \* 安全目标:直接提交,清除 force 标记
                     /\ committed' = t
                     /\ forcedCommitted' = FALSE
                     /\ UNCHANGED <<submitted, auditLog>>
                ELSE IF f
                     THEN \* 低于下限 + force:先写审计再提交
                          \* (admin.rs:622-637,warn! 在 apply 之前)
                          /\ auditLog' = auditLog \cup {t}
                          /\ committed' = t
                          /\ forcedCommitted' = TRUE
                          /\ UNCHANGED submitted
                     ELSE \* 低于下限且无 force:拒绝,状态不变
                          /\ UNCHANGED <<committed, submitted, forcedCommitted, auditLog>>

Next == Submit \/ Resolve

(***************************************************************************)
(* Safety 性质                                                             *)
(***************************************************************************)

\* S1: 已提交 voter 集合永不为空(任何路径,包括 force).
NoEmptyCommitted == committed /= {}

\* S2: 已提交 voter 集合低于下限 ⇒ 必经由 force 且已有审计记录.
\*    (invariants.md: "目标 voter<2 拒,n→0 永不放行" + force 须有审计)
QuorumLossOnlyWithAudit ==
    Cardinality(committed) < MinVoters => (forcedCommitted /\ committed \in auditLog)

\* S3: 审计完备性——低于下限的已提交集合必出现在审计日志里.
AuditComplete ==
    Cardinality(committed) < MinVoters => committed \in auditLog

\* S4: 审计日志单调只增记录的集合里没有空集(空集永不被 force 通过).
AuditNeverEmpty == {} \notin auditLog

Safety ==
    /\ NoEmptyCommitted
    /\ QuorumLossOnlyWithAudit
    /\ AuditComplete
    /\ AuditNeverEmpty

(***************************************************************************)
(* Liveness:合法的(非空)请求最终都会被裁决(提交或拒绝),不会永远挂起.        *)
(* 需要弱公平:只要 Resolve 持续可用就会执行.                                *)
(***************************************************************************)
FairSpec == /\ Init
            /\ [][Next]_vars
            /\ WF_vars(Resolve)

\* 请求有界(MaxRequests):当提交用尽,所有在途请求最终被裁决.
LivenessProp == (submitted = MaxRequests) ~> (inFlight = {})

(***************************************************************************)
(* 状态约束(CI 用小模型;完整空间本地跑). TLC 配置见 MembershipGuard.cfg.    *)
(***************************************************************************)
StateConstraint == submitted <= MaxRequests

\* TLC 只检查 INVARIANTS/PROPERTIES;此行仅作意图声明(TLC 不是定理证明器):
\*   FairSpec => []Safety
=============================================================================

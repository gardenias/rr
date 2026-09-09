---- MODULE MembershipGuardBuggy ----
(***************************************************************************)
(* 故意植入反例(实证用,不进 CI 绿集):移除 quorum guard 的拒绝路径——        *)
(* 低于 MinVoters 的目标在无 force 时也直接提交且无审计.对应 review P0#7  *)
(* 修复前的真实缺陷形态.                                                   *)
(*                                                                         *)
(* TLC 预期抓到: QuorumLossOnlyWithAudit 被违反,反例轨迹终态为             *)
(* committed = 单节点集合 且 forcedCommitted = FALSE 且 auditLog = {}.      *)
(* 实证记录见 formal/tla/README.md "植入反例实证" 一节.                    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANT Nodes
CONSTANT MinVoters
CONSTANT MaxRequests

VARIABLES
    committed, inFlight, submitted, forcedCommitted, auditLog

vars == <<committed, inFlight, submitted, forcedCommitted, auditLog>>

TypeOK ==
    /\ committed \subseteq Nodes
    /\ inFlight \subseteq [target: SUBSET Nodes, force: BOOLEAN]
    /\ submitted \in Nat
    /\ forcedCommitted \in BOOLEAN
    /\ auditLog \subseteq SUBSET Nodes

Init ==
    /\ committed = Nodes
    /\ inFlight = {}
    /\ submitted = 0
    /\ forcedCommitted = FALSE
    /\ auditLog = {}

Submit ==
    /\ submitted < MaxRequests
    /\ \E t \in SUBSET Nodes, f \in {TRUE, FALSE} :
        /\ inFlight' = inFlight \cup {[target |-> t, force |-> f]}
        /\ submitted' = submitted + 1
    /\ UNCHANGED <<committed, forcedCommitted, auditLog>>

Resolve ==
    \E r \in inFlight:
        LET t == r.target
        IN
        /\ inFlight' = inFlight \ {r}
        /\ IF t = {}
           THEN UNCHANGED <<committed, submitted, forcedCommitted, auditLog>>
           ELSE \* BUG: 无 quorum guard——任何非空目标直接提交,无审计.
                /\ committed' = t
                /\ forcedCommitted' = FALSE
                /\ UNCHANGED <<submitted, auditLog>>

Next == Submit \/ Resolve

NoEmptyCommitted == committed /= {}
QuorumLossOnlyWithAudit ==
    Cardinality(committed) < MinVoters => (forcedCommitted /\ committed \in auditLog)
AuditComplete ==
    Cardinality(committed) < MinVoters => committed \in auditLog
AuditNeverEmpty == {} \notin auditLog

Safety ==
    /\ NoEmptyCommitted
    /\ QuorumLossOnlyWithAudit
    /\ AuditComplete
    /\ AuditNeverEmpty

FairSpec == /\ Init /\ [][Next]_vars /\ WF_vars(Resolve)
LivenessProp == (submitted = MaxRequests) ~> (inFlight = {})
StateConstraint == submitted <= MaxRequests

=============================================================================

//! `rr-admin node decommission` — three-step orchestrator that drains
//! a target node from every Raft group it participates in and removes
//! it from both the voter and learner sets.
//!
//! # Group enumeration (cluster-wide)
//!
//! Groups are enumerated from the contacted node's routing table
//! (`GetRoutingTable` — the config-plane view of every partition's
//! group and replica set) UNION the contacted node's local groups
//! (`NodeStatus.group_ids`), so groups where the target participates
//! but the contacted node holds no replica are NOT silently skipped.
//!
//! Limitation: membership probing (`GetGroupMembership`) and membership
//! changes are served LOCALLY by the contacted node — a group it does
//! not host cannot be probed or changed from here. Every such group is
//! reported as `failed` with a re-run hint and the report carries a
//! `caveats` entry (the CLI exits non-zero), regardless of whether the
//! routing table lists the target as a replica: learner-only membership
//! cannot be ruled out from routing metadata. Re-run against a node
//! hosting that group (or its leader).
//!
//! # Workflow
//!
//! For each enumerated group `G`:
//!
//! 1. **Inspect membership** via `AdminService::GetGroupMembership(G)`.
//!    Skip only if the target is absent from both voters and learners.
//! 2. **Refuse voter removal on quorum loss**. The current leader checks
//!    the last/target joint voter config under its membership lock. If the
//!    removal is unsafe the orchestrator aborts without forcing it. Dry-run
//!    uses only an explicitly non-authoritative union-based pre-check.
//! 3. **Drain a target voter** if it is the group's current leader
//!    (decided from `GroupStatus.leader_node_id`, regardless of which
//!    node answered): call `TransferLeader` to a remaining voter, then
//!    poll `GetGroupStatus` until the leader moves off the target (up
//!    to [`DRAIN_TIMEOUT`]). Learner-only targets skip this step.
//! 4. **Remove** by dialing the current leader's admin endpoint and calling
//!    `RemoveGroupMember(G, target)`.
//! 5. **Confirm** through that leader by re-querying membership and asserting
//!    the target is absent from both voters and learners.
//!
//! Every enumerated group is processed; a failure in one group is
//! recorded in the report and does NOT stop the others (re-run is
//! idempotent given `RemoveGroupMember` semantics).
//!
//! # Dry run
//!
//! `--dry-run` skips steps 3-5 and prints what would happen. Useful
//! for checking quorum impact before committing.

use crate::AdminClient;
use anyhow::{anyhow, Result};
use rr_core::membership::{membership_change_is_safe, MIN_VOTERS_AFTER_REMOVAL};
use rr_network::proto::admin::{
    DrainRequest, Empty, GroupId, NodeMember, RemoveGroupMemberRequest,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tonic::Code;

/// Maximum time we wait for a transferred leadership to land on the
/// new leader before giving up on the drain step. ~10s covers a
/// healthy 3-5 node cluster + one election timeout.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often we poll `GetGroupStatus` while waiting for drain to
/// complete. Mirrors openraft's tick cadence.
pub const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Per-group outcome line for the report.
#[derive(Debug, Clone, Serialize)]
pub struct DecommissionGroupResult {
    pub group_id: u64,
    pub action: GroupAction,
    pub message: String,
}

/// What happened to a particular group during the workflow.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupAction {
    /// Target was absent from both voters and learners.
    Skipped,
    /// Removing target would have left fewer than
    /// `MIN_VOTERS_AFTER_REMOVAL` voters; aborted.
    AbortedQuorumLoss,
    /// `--dry-run`: would have removed; nothing changed.
    WouldRemove,
    /// Target was leader; transferred + removed successfully.
    DrainedAndRemoved,
    /// Target was a follower or learner; removed directly.
    Removed,
    /// A step failed — see `message`.
    Failed,
}

/// Top-level decommission report.
#[derive(Debug, Clone, Serialize)]
pub struct DecommissionReport {
    pub target_node_id: u64,
    pub dry_run: bool,
    pub groups: Vec<DecommissionGroupResult>,
    /// Operational caveats about coverage gaps (e.g. groups that could
    /// not be probed from the contacted node). Empty when the run had
    /// full cluster coverage. Printed prominently by the CLI.
    pub caveats: Vec<String>,
}

impl DecommissionReport {
    /// Whether at least one group was either Removed, DrainedAndRemoved,
    /// or WouldRemove. Useful for CI/CD wrapper scripts.
    pub fn made_progress(&self) -> bool {
        self.groups.iter().any(|g| {
            matches!(
                g.action,
                GroupAction::Removed | GroupAction::DrainedAndRemoved | GroupAction::WouldRemove
            )
        })
    }

    /// Whether any group failed, was blocked by quorum policy, or could
    /// not be checked. Any of these makes the decommission unsuccessful.
    pub fn has_failures(&self) -> bool {
        !self.caveats.is_empty()
            || self.groups.iter().any(|g| {
                matches!(
                    g.action,
                    GroupAction::Failed | GroupAction::AbortedQuorumLoss
                )
            })
    }
}

/// Per-group workflow outcome, plus whether the group could not even be
/// probed from the contacted node (a coverage gap — see module docs).
struct GroupOutcome {
    result: DecommissionGroupResult,
    coverage_gap: bool,
}

impl GroupOutcome {
    fn ok(result: DecommissionGroupResult) -> Self {
        Self {
            result,
            coverage_gap: false,
        }
    }
}

/// Merge routing-table partitions with the contacted node's local groups
/// into the enumeration set: `group_id → routing table lists the target
/// as a replica`. Local-only groups are included with `false` (the
/// membership probe decides); a group appearing in several partitions is
/// marked if the target is a replica in ANY of them.
fn enumerate_groups(
    partitions: impl IntoIterator<Item = (u64, Vec<u64>)>,
    local_group_ids: impl IntoIterator<Item = u64>,
    target_node_id: u64,
) -> BTreeMap<u64, bool> {
    let mut groups = BTreeMap::new();
    for (group_id, replicas) in partitions {
        *groups.entry(group_id).or_insert(false) |= replicas.contains(&target_node_id);
    }
    for g in local_group_ids {
        groups.entry(g).or_insert(false);
    }
    groups
}

/// Whether the target currently leads the group, decided from the
/// group's reported `leader_node_id` regardless of which node answered
/// the status RPC (a follower answer still carries the leader's id;
/// `0` means "no known leader").
fn target_is_leader(leader_node_id: u64, target_node_id: u64) -> bool {
    leader_node_id != 0 && leader_node_id == target_node_id
}

/// Run the decommission workflow against `client` for `target_node_id`.
///
/// `client` MUST point at a healthy admin endpoint of any cluster member.
/// Group enumeration and status polling start through it; removal and
/// confirmation use the group's current leader.
///
/// `dial_admin` dials an arbitrary node's admin endpoint with the same
/// TLS/token configuration. It is needed both for the leadership drain via
/// the TARGET's OWN self-only admin API and for the current leader after
/// that drain (and for groups whose leader was never the target).
pub async fn decommission_node<F, Fut>(
    client: &mut AdminClient,
    dial_admin: F,
    target_node_id: u64,
    dry_run: bool,
) -> Result<DecommissionReport>
where
    F: Fn(&str) -> Fut,
    Fut: std::future::Future<Output = Result<AdminClient>>,
{
    let node_status = client.get_node_status(Empty {}).await?.into_inner();

    let mut caveats = Vec::new();
    let partitions = match client.get_routing_table(Empty {}).await {
        Ok(rt) => rt
            .into_inner()
            .partitions
            .into_iter()
            .map(|p| (p.group_id, p.replica_node_ids))
            .collect::<Vec<_>>(),
        Err(status) => {
            caveats.push(format!(
                "cluster-wide group enumeration (GetRoutingTable) failed [{}]: {}; \
                 only the contacted node's LOCAL groups were examined — groups hosted \
                 only elsewhere were NOT checked",
                status.code(),
                status.message(),
            ));
            Vec::new()
        }
    };
    let groups = enumerate_groups(partitions, node_status.group_ids, target_node_id);

    let mut report = DecommissionReport {
        target_node_id,
        dry_run,
        groups: Vec::with_capacity(groups.len()),
        caveats,
    };

    let mut coverage_gaps = Vec::new();
    for (group_id, target_is_replica) in groups {
        let result = decommission_one_group(
            client,
            &dial_admin,
            group_id,
            target_node_id,
            target_is_replica,
            dry_run,
        )
        .await;
        report.groups.push(match result {
            Ok(outcome) => {
                if outcome.coverage_gap {
                    coverage_gaps.push(group_id);
                }
                outcome.result
            }
            Err(e) => DecommissionGroupResult {
                group_id,
                action: GroupAction::Failed,
                message: format!("orchestrator error: {e}"),
            },
        });
    }

    if !coverage_gaps.is_empty() {
        report.caveats.push(format!(
            "groups {coverage_gaps:?} could not be probed from the contacted node — membership \
             is UNKNOWN and they were NOT decommissioned; re-run against a node hosting them \
             (or their leader)"
        ));
    }

    Ok(report)
}

async fn decommission_one_group<F, Fut>(
    client: &mut AdminClient,
    dial_admin: &F,
    group_id: u64,
    target_node_id: u64,
    target_is_replica: bool,
    dry_run: bool,
) -> Result<GroupOutcome>
where
    F: Fn(&str) -> Fut,
    Fut: std::future::Future<Output = Result<AdminClient>>,
{
    let membership = match client.get_group_membership(GroupId { group_id }).await {
        Ok(resp) => resp.into_inner(),
        Err(status) if status.code() == Code::NotFound => {
            let routing_note = if target_is_replica {
                "the routing table lists the target as a replica"
            } else {
                "a learner-only target need not appear in routing-table replicas"
            };
            return Ok(GroupOutcome {
                result: DecommissionGroupResult {
                    group_id,
                    action: GroupAction::Failed,
                    message: format!(
                        "contacted node does not host group {group_id}; membership is UNKNOWN \
                         ({routing_note}) — NOT decommissioned; re-run against a node hosting \
                         this group (or its leader)"
                    ),
                },
                coverage_gap: true,
            });
        }
        Err(status) => return Err(status.into()),
    };

    let voter_ids: Vec<u64> = membership.voters.iter().map(|m| m.node_id).collect();
    let learner_ids: Vec<u64> = membership.learners.iter().map(|m| m.node_id).collect();
    let target_is_voter = voter_ids.contains(&target_node_id);
    let target_is_learner = learner_ids.contains(&target_node_id);
    if !target_is_voter && !target_is_learner {
        return Ok(GroupOutcome::ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::Skipped,
            message: "target is absent from both voters and learners".to_string(),
        }));
    }

    let remaining: Vec<u64> = voter_ids
        .iter()
        .copied()
        .filter(|id| *id != target_node_id)
        .collect();
    let remaining_learners: Vec<u64> = learner_ids
        .iter()
        .copied()
        .filter(|id| *id != target_node_id)
        .collect();
    // Dry-run cannot ask the leader for its locked decision. Its union-based
    // estimate is explicitly non-authoritative for joint configs; live removal
    // always relies on the server's target-config policy instead.
    if dry_run && target_is_voter && !membership_change_is_safe(remaining.len()) {
        return Ok(GroupOutcome::ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::AbortedQuorumLoss,
            message: format!(
                "dry-run pre-check estimates {} voters (< {} minimum); add a new voter first",
                remaining.len(),
                MIN_VOTERS_AFTER_REMOVAL
            ),
        }));
    }

    if dry_run {
        let message = if target_is_voter {
            format!("dry-run: would change voters from {voter_ids:?} to {remaining:?}")
        } else {
            format!("dry-run: would change learners from {learner_ids:?} to {remaining_learners:?}")
        };
        return Ok(GroupOutcome::ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::WouldRemove,
            message,
        }));
    }

    let mut drained_via_transfer = false;
    let mut drain_caveat: Option<String> = None;
    let mut removal_leader_id = None;
    if target_is_voter {
        let group_status = client
            .get_group_status(GroupId { group_id })
            .await?
            .into_inner();
        removal_leader_id =
            (group_status.leader_node_id != 0).then_some(group_status.leader_node_id);
        if target_is_leader(group_status.leader_node_id, target_node_id) {
            match drive_drain_via_target(
                client,
                dial_admin,
                &membership.voters,
                group_id,
                target_node_id,
            )
            .await
            {
                Ok(new_leader_id) => {
                    drained_via_transfer = true;
                    removal_leader_id = Some(new_leader_id);
                }
                Err(reason) => {
                    // The drain is an availability optimization; the membership
                    // change below is the authoritative removal. A target that
                    // cannot be drained is usually already down (the election
                    // settles leadership on its own) — proceed, but say so loudly
                    // instead of failing the whole group like the old
                    // `transfer_leader`-via-contacted-node path always did.
                    drain_caveat = Some(format!(
                        "leadership drain could not be driven ({reason}); proceeded with the \
                         membership change — the raft election settles leadership"
                    ));
                }
            }
        }
    }

    let mut leader_client = match removal_leader_id {
        Some(leader_id) => {
            let leader_addr = membership
                .voters
                .iter()
                .find(|member| member.node_id == leader_id)
                .map(|member| member.addr.clone())
                .filter(|addr| !addr.is_empty())
                .ok_or_else(|| anyhow!("leader {leader_id}'s raft address is unknown"))?;
            let admin_addr = crate::default_admin_addr(&leader_addr)?;
            Some(dial_admin(&admin_addr).await?)
        }
        None => None,
    };
    let removal_client = leader_client.as_mut().unwrap_or(client);
    if let Err(status) = removal_client
        .remove_group_member(RemoveGroupMemberRequest {
            group_id,
            node_id: target_node_id,
            // Decommission deliberately has no force channel: its contract
            // is "add a fresh voter first, then remove" (see module docs).
            force: false,
        })
        .await
    {
        if matches!(
            status.code(),
            Code::FailedPrecondition | Code::InvalidArgument
        ) {
            return Ok(GroupOutcome::ok(DecommissionGroupResult {
                group_id,
                action: GroupAction::AbortedQuorumLoss,
                message: format!("leader rejected removal under its locked voter policy: {status}"),
            }));
        }
        return Err(status.into());
    }

    let confirm = removal_client
        .get_group_membership(GroupId { group_id })
        .await?
        .into_inner();
    let still_voter = confirm.voters.iter().any(|m| m.node_id == target_node_id);
    let still_learner = confirm.learners.iter().any(|m| m.node_id == target_node_id);
    if still_voter || still_learner {
        return Ok(GroupOutcome::ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::Failed,
            message: format!(
                "post-removal membership still lists target (voter: {still_voter}, learner: {still_learner}); re-run"
            ),
        }));
    }

    let change_summary = if target_is_voter {
        format!("voters: {voter_ids:?} → {remaining:?}")
    } else {
        format!("learners: {learner_ids:?} → {remaining_learners:?}")
    };
    Ok(GroupOutcome::ok(DecommissionGroupResult {
        group_id,
        action: if drained_via_transfer {
            GroupAction::DrainedAndRemoved
        } else {
            GroupAction::Removed
        },
        message: format!(
            "{change_summary}; target absent from voters and learners{}{}",
            if drained_via_transfer {
                " (after leadership transfer)"
            } else {
                ""
            },
            drain_caveat
                .map(|c| format!(" — WARNING: {c}"))
                .unwrap_or_default()
        ),
    }))
}

/// Drive the leadership drain OFF the target via the target's OWN admin
/// endpoint: `TransferLeader`/`DrainNode` are self-only RPCs server-side,
/// so asking the contacted node to transfer leadership to a third node is
/// always refused. The target's admin address is derived from its raft
/// address with the deployment-wide port+1000 convention.
async fn drive_drain_via_target<F, Fut>(
    client: &mut AdminClient,
    dial_admin: &F,
    voters: &[NodeMember],
    group_id: u64,
    target_node_id: u64,
) -> Result<u64>
where
    F: Fn(&str) -> Fut,
    Fut: std::future::Future<Output = Result<AdminClient>>,
{
    let target_addr = voters
        .iter()
        .find(|m| m.node_id == target_node_id)
        .map(|m| m.addr.clone())
        .filter(|a| !a.is_empty())
        .ok_or_else(|| anyhow!("target's raft address is unknown (not in membership)"))?;
    let admin_addr = crate::default_admin_addr(&target_addr)?;
    let mut target_client = dial_admin(&admin_addr).await?;
    target_client
        .drain_node(DrainRequest {
            node_id: target_node_id,
            timeout_secs: DRAIN_TIMEOUT.as_secs() as u32,
        })
        .await?;
    wait_for_drain(client, group_id, target_node_id).await
}

async fn wait_for_drain(
    client: &mut AdminClient,
    group_id: u64,
    target_node_id: u64,
) -> Result<u64> {
    let deadline = Instant::now() + DRAIN_TIMEOUT;
    while Instant::now() < deadline {
        let status = client
            .get_group_status(GroupId { group_id })
            .await?
            .into_inner();
        if status.leader_node_id != 0 && status.leader_node_id != target_node_id {
            return Ok(status.leader_node_id);
        }
        tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
    }
    Err(anyhow!(
        "leadership did not move off node {target_node_id} for group {group_id} within {DRAIN_TIMEOUT:?}",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{connect, make_admin_client};

    #[test]
    fn report_made_progress_after_remove() {
        let report = DecommissionReport {
            target_node_id: 4,
            dry_run: false,
            groups: vec![DecommissionGroupResult {
                group_id: 1,
                action: GroupAction::Removed,
                message: "ok".into(),
            }],
            caveats: Vec::new(),
        };
        assert!(report.made_progress());
        assert!(!report.has_failures());
    }

    #[test]
    fn report_made_progress_after_drained_and_removed() {
        let report = DecommissionReport {
            target_node_id: 4,
            dry_run: false,
            groups: vec![DecommissionGroupResult {
                group_id: 1,
                action: GroupAction::DrainedAndRemoved,
                message: "ok".into(),
            }],
            caveats: Vec::new(),
        };
        assert!(report.made_progress());
    }

    #[test]
    fn report_dry_run_progress_counts_would_remove() {
        let report = DecommissionReport {
            target_node_id: 4,
            dry_run: true,
            groups: vec![DecommissionGroupResult {
                group_id: 1,
                action: GroupAction::WouldRemove,
                message: "dry".into(),
            }],
            caveats: Vec::new(),
        };
        assert!(report.made_progress());
    }

    #[test]
    fn report_skipped_only_does_not_count_as_progress() {
        let report = DecommissionReport {
            target_node_id: 4,
            dry_run: false,
            groups: vec![DecommissionGroupResult {
                group_id: 1,
                action: GroupAction::Skipped,
                message: "not a voter".into(),
            }],
            caveats: Vec::new(),
        };
        assert!(!report.made_progress());
    }

    // INV-7: change-membership must not shrink a group's voters below quorum (>= 2) without explicit force + audit.
    #[test]
    fn report_quorum_loss_does_not_count_as_progress() {
        let report = DecommissionReport {
            target_node_id: 4,
            dry_run: false,
            groups: vec![DecommissionGroupResult {
                group_id: 1,
                action: GroupAction::AbortedQuorumLoss,
                message: "not enough voters".into(),
            }],
            caveats: Vec::new(),
        };
        assert!(!report.made_progress());
        assert!(report.has_failures());
    }

    #[test]
    fn report_coverage_gap_is_a_failure() {
        let report = DecommissionReport {
            target_node_id: 4,
            dry_run: false,
            groups: Vec::new(),
            caveats: vec!["group 9 was not checked".into()],
        };
        assert!(report.has_failures());
    }

    #[test]
    fn partial_completion_is_not_successful() {
        let report = DecommissionReport {
            target_node_id: 4,
            dry_run: false,
            groups: vec![
                DecommissionGroupResult {
                    group_id: 1,
                    action: GroupAction::Removed,
                    message: "removed".into(),
                },
                DecommissionGroupResult {
                    group_id: 2,
                    action: GroupAction::AbortedQuorumLoss,
                    message: "blocked".into(),
                },
            ],
            caveats: Vec::new(),
        };
        assert!(report.made_progress());
        assert!(report.has_failures());
    }

    #[test]
    fn report_has_failures_flag_set_on_failure() {
        let report = DecommissionReport {
            target_node_id: 4,
            dry_run: false,
            groups: vec![DecommissionGroupResult {
                group_id: 1,
                action: GroupAction::Failed,
                message: "rpc error".into(),
            }],
            caveats: Vec::new(),
        };
        assert!(report.has_failures());
    }

    // INV-7: change-membership must not shrink a group's voters below quorum (>= 2) without explicit force + audit.
    #[test]
    fn min_voters_after_removal_constant_is_two() {
        // Pins the shared policy constant (rr-core) the client-side
        // pre-check references — the server enforces the same value.
        assert_eq!(MIN_VOTERS_AFTER_REMOVAL, 2);
    }

    /// Regression: enumeration must be cluster-wide (routing table), not
    /// limited to the contacted node's local groups — a group where the
    /// target holds a replica but the contacted node holds none must show
    /// up marked `target_is_replica = true`.
    #[test]
    fn enumerate_groups_unions_routing_table_and_local_groups() {
        let partitions = vec![
            (1u64, vec![1, 2, 3]), // target 4 absent
            (2u64, vec![1, 4]),    // target present
            (2u64, vec![4, 5]),    // same group, second partition → stays true
        ];
        let local = vec![1u64, 9u64]; // group 9 is local-only
        let groups = enumerate_groups(partitions, local, 4);
        assert_eq!(groups.get(&1), Some(&false));
        assert_eq!(groups.get(&2), Some(&true));
        assert_eq!(groups.get(&9), Some(&false));
        assert_eq!(groups.len(), 3);
    }

    #[test]
    fn enumerate_groups_marks_target_seen_in_any_partition() {
        let partitions = vec![(3u64, vec![1, 2]), (3u64, vec![4])];
        let groups = enumerate_groups(partitions, [], 4);
        assert_eq!(groups.get(&3), Some(&true));
    }

    /// Regression: drain is driven off the group's actual `leader_node_id`,
    /// regardless of which node answered the status RPC.
    #[test]
    fn target_is_leader_follows_leader_node_id() {
        assert!(target_is_leader(4, 4));
        assert!(!target_is_leader(1, 4)); // leader is someone else
        assert!(!target_is_leader(0, 4)); // no known leader
    }

    use rr_network::proto::admin::admin_service_server::{AdminService, AdminServiceServer};
    use rr_network::proto::admin::{
        AddLearnerRequest, ChangeMembershipRequest, DrainResult, FormationStatus, GroupList,
        GroupMembership, GroupStatus, MetricsSummary, NodeStatus, OnlineBackupChunk,
        OnlineBackupRequest, RemoveGroupMemberRequest, ResetIngestorOffsetRequest,
        RoutingTableSnapshot, SnapshotStatus, TransferLeaderRequest,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tonic::{Request, Response, Status};

    #[derive(Clone)]
    struct StubAdminService {
        state: Arc<StubAdminState>,
    }

    struct StubAdminState {
        membership: tokio::sync::Mutex<GroupMembership>,
        remove_requests: Mutex<Vec<RemoveGroupMemberRequest>>,
        status_calls: AtomicUsize,
        target_node_id: u64,
        leave_target_learner: bool,
        membership_available: bool,
    }

    impl StubAdminService {
        fn new(
            voters: &[u64],
            learners: &[u64],
            target_node_id: u64,
            leave_target_learner: bool,
        ) -> Self {
            Self::with_availability(voters, learners, target_node_id, leave_target_learner, true)
        }

        fn unavailable(target_node_id: u64) -> Self {
            Self::with_availability(&[], &[], target_node_id, false, false)
        }

        fn with_availability(
            voters: &[u64],
            learners: &[u64],
            target_node_id: u64,
            leave_target_learner: bool,
            membership_available: bool,
        ) -> Self {
            Self {
                state: Arc::new(StubAdminState {
                    membership: tokio::sync::Mutex::new(GroupMembership {
                        group_id: 1,
                        voters: voters.iter().map(|id| member(*id)).collect(),
                        learners: learners.iter().map(|id| member(*id)).collect(),
                    }),
                    remove_requests: Mutex::new(Vec::new()),
                    status_calls: AtomicUsize::new(0),
                    target_node_id,
                    leave_target_learner,
                    membership_available,
                }),
            }
        }
    }

    fn member(node_id: u64) -> NodeMember {
        NodeMember {
            node_id,
            addr: format!("127.0.0.1:{}", 51000 + node_id),
        }
    }

    #[tonic::async_trait]
    impl AdminService for StubAdminService {
        async fn get_node_status(
            &self,
            _request: Request<Empty>,
        ) -> std::result::Result<Response<NodeStatus>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn get_group_status(
            &self,
            request: Request<GroupId>,
        ) -> std::result::Result<Response<GroupStatus>, Status> {
            self.state.status_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(GroupStatus {
                group_id: request.into_inner().group_id,
                is_leader: true,
                current_term: 1,
                last_log_index: 1,
                leader_node_id: 1,
            }))
        }

        async fn list_groups(
            &self,
            _request: Request<Empty>,
        ) -> std::result::Result<Response<GroupList>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn get_snapshot_status(
            &self,
            _request: Request<GroupId>,
        ) -> std::result::Result<Response<SnapshotStatus>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn trigger_snapshot(
            &self,
            _request: Request<GroupId>,
        ) -> std::result::Result<Response<Empty>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn transfer_leader(
            &self,
            _request: Request<TransferLeaderRequest>,
        ) -> std::result::Result<Response<Empty>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn drain_node(
            &self,
            _request: Request<DrainRequest>,
        ) -> std::result::Result<Response<DrainResult>, Status> {
            Err(Status::unimplemented("learner-only removal must not drain"))
        }

        async fn add_learner(
            &self,
            _request: Request<AddLearnerRequest>,
        ) -> std::result::Result<Response<Empty>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn change_membership(
            &self,
            request: Request<ChangeMembershipRequest>,
        ) -> std::result::Result<Response<Empty>, Status> {
            let request = request.into_inner();
            let mut membership = self.state.membership.lock().await;
            membership.voters = request
                .voter_node_ids
                .iter()
                .map(|id| member(*id))
                .collect();
            if self.state.leave_target_learner {
                membership.learners.push(member(self.state.target_node_id));
            }
            Ok(Response::new(Empty {}))
        }

        async fn remove_group_member(
            &self,
            request: Request<RemoveGroupMemberRequest>,
        ) -> std::result::Result<Response<Empty>, Status> {
            let request = request.into_inner();
            self.state
                .remove_requests
                .lock()
                .unwrap()
                .push(request.clone());
            let mut membership = self.state.membership.lock().await;
            membership.voters.retain(|m| m.node_id != request.node_id);
            membership.learners.retain(|m| m.node_id != request.node_id);
            if self.state.leave_target_learner {
                membership.learners.push(member(request.node_id));
            }
            Ok(Response::new(Empty {}))
        }

        async fn reset_ingestor_offset(
            &self,
            _request: Request<ResetIngestorOffsetRequest>,
        ) -> std::result::Result<Response<Empty>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn get_group_membership(
            &self,
            _request: Request<GroupId>,
        ) -> std::result::Result<Response<GroupMembership>, Status> {
            if !self.state.membership_available {
                return Err(Status::not_found("group is not hosted here"));
            }
            Ok(Response::new(self.state.membership.lock().await.clone()))
        }

        type OnlineBackupStream = std::pin::Pin<
            Box<dyn futures::Stream<Item = std::result::Result<OnlineBackupChunk, Status>> + Send>,
        >;

        async fn online_backup(
            &self,
            _request: Request<OnlineBackupRequest>,
        ) -> std::result::Result<Response<Self::OnlineBackupStream>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn get_routing_table(
            &self,
            _request: Request<Empty>,
        ) -> std::result::Result<Response<RoutingTableSnapshot>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn get_metrics_summary(
            &self,
            _request: Request<Empty>,
        ) -> std::result::Result<Response<MetricsSummary>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn get_formation_status(
            &self,
            _request: Request<Empty>,
        ) -> std::result::Result<Response<FormationStatus>, Status> {
            Err(Status::unimplemented("not used"))
        }
    }

    async fn run_one_group(
        service: &StubAdminService,
        target_node_id: u64,
        target_is_replica: bool,
        dry_run: bool,
    ) -> GroupOutcome {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub admin service");
        let addr = listener.local_addr().expect("stub admin address");
        drop(listener);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(AdminServiceServer::new(service.clone()))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                }),
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        let mut client = make_admin_client(
            connect(&addr.to_string(), None)
                .await
                .expect("connect to stub admin service"),
            &None,
        );
        let dial_admin = |_addr: &str| async move {
            let channel = connect(&addr.to_string(), None).await?;
            Ok(make_admin_client(channel, &None))
        };
        let outcome = decommission_one_group(
            &mut client,
            &dial_admin,
            1,
            target_node_id,
            target_is_replica,
            dry_run,
        )
        .await
        .expect("group workflow must return a report outcome");
        shutdown_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
        outcome
    }

    #[tokio::test]
    async fn learner_only_target_is_removed_without_quorum_or_leadership_steps() {
        let service = StubAdminService::new(&[1], &[4], 4, false);
        let outcome = run_one_group(&service, 4, true, false).await;

        assert!(matches!(outcome.result.action, GroupAction::Removed));
        assert_eq!(service.state.status_calls.load(Ordering::SeqCst), 0);
        let requests = service.state.remove_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].group_id, 1);
        assert_eq!(requests[0].node_id, 4);
        assert!(!requests[0].force);
        drop(requests);
        assert!(service.state.membership.lock().await.learners.is_empty());
    }

    #[tokio::test]
    async fn confirmation_rejects_target_left_as_learner() {
        let service = StubAdminService::new(&[1, 2, 3], &[], 3, true);
        let outcome = run_one_group(&service, 3, true, false).await;

        assert!(matches!(outcome.result.action, GroupAction::Failed));
        assert!(outcome.result.message.contains("learner"));
        assert_eq!(service.state.remove_requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unhosted_learner_group_is_coverage_gap_regardless_of_routing_metadata() {
        for target_is_replica in [false, true] {
            let service = StubAdminService::unavailable(4);
            let outcome = run_one_group(&service, 4, target_is_replica, false).await;

            assert!(matches!(outcome.result.action, GroupAction::Failed));
            assert!(outcome.coverage_gap);
            assert!(outcome.result.message.contains("UNKNOWN"));
            assert!(service.state.remove_requests.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn unhosted_group_dry_run_is_a_coverage_gap() {
        let service = StubAdminService::unavailable(4);
        let outcome = run_one_group(&service, 4, false, true).await;

        assert!(matches!(outcome.result.action, GroupAction::Failed));
        assert!(outcome.coverage_gap);
        assert!(service.state.remove_requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn learner_summary_preserves_unrelated_learners() {
        let service = StubAdminService::new(&[1], &[4, 5], 4, false);
        let outcome = run_one_group(&service, 4, true, false).await;

        assert!(matches!(outcome.result.action, GroupAction::Removed));
        assert!(outcome.result.message.contains("learners: [4, 5] → [5]"));
        let membership = service.state.membership.lock().await;
        let learners: Vec<u64> = membership.learners.iter().map(|m| m.node_id).collect();
        assert_eq!(learners, vec![5]);
    }

    #[tokio::test]
    async fn learner_dry_run_reports_remaining_learners() {
        let service = StubAdminService::new(&[1], &[4, 5], 4, false);
        let outcome = run_one_group(&service, 4, true, true).await;

        assert!(matches!(outcome.result.action, GroupAction::WouldRemove));
        assert!(outcome
            .result
            .message
            .contains("dry-run: would change learners from [4, 5] to [5]"));
        assert!(service.state.remove_requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn absent_node_retry_is_a_successful_noop() {
        let service = StubAdminService::new(&[1, 2, 3], &[], 3, false);
        let outcome = run_one_group(&service, 4, true, false).await;

        assert!(matches!(outcome.result.action, GroupAction::Skipped));
        assert!(!outcome.coverage_gap);
        assert!(service.state.remove_requests.lock().unwrap().is_empty());
        assert_eq!(service.state.status_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn decommission_current_leader_uses_new_leader_for_all_groups() {
        let (nodes, admin_ports) = build_decommission_leader_cluster()
            .await
            .expect("build real two-group cluster");
        let target_node_id = wait_for_decommission_leader(&nodes, 1)
            .await
            .expect("group 1 must elect a leader");
        let _ = wait_for_decommission_leader(&nodes, 2)
            .await
            .expect("group 2 must elect a leader");
        let mut client = decommission_test_admin(&format!(
            "127.0.0.1:{}",
            admin_ports[(target_node_id - 1) as usize]
        ))
        .await
        .expect("connect to the target leader");

        let report = decommission_node(
            &mut client,
            |addr: &str| {
                let addr = addr.to_owned();
                async move { decommission_test_admin(&addr).await }
            },
            target_node_id,
            false,
        )
        .await
        .expect("decommission must return a report");

        assert!(!report.has_failures(), "decommission failed: {report:?}");
        assert_eq!(report.groups.len(), 2, "{report:?}");
        assert!(
            report
                .groups
                .iter()
                .any(|group| matches!(group.action, GroupAction::DrainedAndRemoved)),
            "the current leader must be drained before removal: {report:?}"
        );
        for group_id in [1, 2] {
            let remaining_id = (1..=3).find(|id| *id != target_node_id).unwrap();
            let mut remaining_client = decommission_test_admin(&format!(
                "127.0.0.1:{}",
                admin_ports[(remaining_id - 1) as usize]
            ))
            .await
            .unwrap();
            let membership = remaining_client
                .get_group_membership(GroupId { group_id })
                .await
                .unwrap()
                .into_inner();
            assert!(
                membership
                    .voters
                    .iter()
                    .chain(&membership.learners)
                    .all(|member| member.node_id != target_node_id),
                "target remains in group {group_id}: {membership:?}"
            );
        }

        for node in &nodes {
            let _ = node.shutdown().await;
        }
    }

    async fn decommission_test_admin(addr: &str) -> Result<AdminClient> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match connect(addr, None).await {
                Ok(channel) => return Ok(make_admin_client(channel, &None)),
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn wait_for_decommission_leader(
        nodes: &[Arc<rr_server::node::RrNode>],
        group_id: u64,
    ) -> Result<u64> {
        use openraft::type_config::async_runtime::WatchReceiver;

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            for node in nodes {
                if let Some(raft) = node.group_manager().get_raft(group_id).await {
                    if let Some(leader) = raft.metrics().borrow_watched().current_leader {
                        return Ok(leader);
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err(anyhow!("timed out waiting for group {group_id} leader"));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn build_decommission_leader_cluster(
    ) -> Result<(Vec<Arc<rr_server::node::RrNode>>, Vec<u16>)> {
        use rr_config::mem::MemConfigProvider;
        use rr_core::config::ConfigProvider;
        use rr_core::storage::StorageEngine;
        use rr_core::types::PartitionConfig;
        use rr_raft::group_manager::RaftGroupManager;
        use rr_raft::network_factory::{ChannelPool, ChannelPoolLike};
        use rr_router::router::ShardRouter;
        use rr_server::config::NodeConfig;
        use rr_server::dispatch::ClientDispatchImpl;
        use rr_server::lookup::RaftGroupLookupImpl;
        use rr_storage::mem::MemStorage;

        let config_provider = Arc::new(MemConfigProvider::new());
        for group_id in [1, 2] {
            config_provider
                .add_partition(
                    group_id,
                    PartitionConfig::SingleId(group_id),
                    group_id,
                    vec![1, 2, 3],
                )
                .await;
        }

        let mut node_addrs = Vec::new();
        let mut admin_ports = Vec::new();
        for node_id in [1, 2, 3] {
            let (raft_port, admin_port) = loop {
                let raft_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                let raft_port = raft_listener.local_addr()?.port();
                drop(raft_listener);
                let Some(admin_port) = raft_port.checked_add(1000) else {
                    continue;
                };
                if let Ok(admin_listener) =
                    tokio::net::TcpListener::bind(("127.0.0.1", admin_port)).await
                {
                    drop(admin_listener);
                    break (raft_port, admin_port);
                }
            };
            node_addrs.push((node_id, format!("127.0.0.1:{raft_port}")));
            admin_ports.push(admin_port);
            config_provider
                .add_node(
                    node_id,
                    rr_core::types::NodeAddr {
                        addr: format!("127.0.0.1:{raft_port}"),
                    },
                )
                .await;
        }

        let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
        let mut nodes = Vec::new();
        for ((node_id, addr), admin_port) in node_addrs.iter().zip(&admin_ports) {
            let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
            let provider: Arc<dyn ConfigProvider> = Arc::clone(&config_provider) as _;
            let mgr = Arc::new(RaftGroupManager::new_with_network(
                *node_id,
                Box::new(|_| Box::new(rr_server::builtin_service::EchoStateMachine::new())),
                pool.clone(),
            ));
            let router = Arc::new(ShardRouter::new(Arc::clone(&provider)));
            let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
            let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));
            let config = NodeConfig::new(
                *node_id,
                addr.clone(),
                storage,
                provider,
                Box::new(|_| Box::new(rr_server::builtin_service::EchoStateMachine::new())),
            )
            .with_group_manager(Arc::clone(&mgr))
            .with_raft_lookup(lookup)
            .with_client_dispatch(dispatch)
            .with_admin_port(*admin_port)
            .with_allow_insecure(true);
            let node = Arc::new(rr_server::node::RrNode::new(config)?);
            node.start().await?;
            nodes.push(node);
        }

        for group_id in [1, 2] {
            let members = node_addrs
                .iter()
                .map(|(node_id, addr)| (*node_id, openraft::BasicNode { addr: addr.clone() }))
                .collect::<BTreeMap<_, _>>();
            let raft = nodes[0]
                .group_manager()
                .get_raft(group_id)
                .await
                .expect("node 1 must host every group");
            let _ = raft.initialize(members).await;
        }
        Ok((nodes, admin_ports))
    }
}

//! `rr-admin node decommission` — three-step orchestrator that drains
//! a target node from every Raft group it participates in and removes
//! it from the voter set.
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
//! not host cannot be probed or changed from here. When the routing
//! table lists the target as a replica of such a group, the group is
//! reported as `failed` with a re-run hint and the report carries a
//! `caveats` entry (the CLI exits non-zero): re-run against a node
//! hosting that group (or its leader).
//!
//! # Workflow
//!
//! For each enumerated group `G`:
//!
//! 1. **Inspect membership** via `AdminService::GetGroupMembership(G)`.
//!    Skip if the target node isn't a voter (already drained, or only
//!    a learner — neither blocks decommission).
//! 2. **Refuse on quorum loss**. If removing the target would drop
//!    `voters.len()` below 2 the orchestrator aborts WITHOUT touching
//!    the group — operators must explicitly accept the risk by adding
//!    a new voter first. Single-node clusters are the exception
//!    (n=1 → n=0 means "tear down the cluster"; we still refuse and
//!    require the operator to use `node-reset` instead).
//! 3. **Drain** if the target is the group's current leader (decided
//!    from `GroupStatus.leader_node_id`, regardless of which node
//!    answered): call `TransferLeader` to a remaining voter, then poll
//!    `GetGroupStatus` until the leader moves off the target (up to
//!    [`DRAIN_TIMEOUT`]).
//! 4. **Remove** by calling `ChangeMembership(G, voters - {target})`.
//! 5. **Confirm** by re-querying membership and asserting the target
//!    is no longer a voter.
//!
//! Every enumerated group is processed; a failure in one group is
//! recorded in the report and does NOT stop the others (re-run is
//! idempotent given the framework's `change_membership` semantics).
//!
//! # Dry run
//!
//! `--dry-run` skips steps 3-5 and prints what would happen. Useful
//! for checking quorum impact before committing.

use crate::AdminClient;
use anyhow::{anyhow, Result};
use rr_core::membership::{membership_change_is_safe, MIN_VOTERS_AFTER_REMOVAL};
use rr_network::proto::admin::{
    ChangeMembershipRequest, Empty, GroupId, NodeMember, TransferLeaderRequest,
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
    /// Target wasn't a voter (already drained or never present).
    Skipped,
    /// Removing target would have left fewer than
    /// `MIN_VOTERS_AFTER_REMOVAL` voters; aborted.
    AbortedQuorumLoss,
    /// `--dry-run`: would have removed; nothing changed.
    WouldRemove,
    /// Target was leader; transferred + removed successfully.
    DrainedAndRemoved,
    /// Target was a follower; removed directly.
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

    /// Whether any group failed mid-workflow. Operator should re-run
    /// after investigating.
    pub fn has_failures(&self) -> bool {
        self.groups
            .iter()
            .any(|g| matches!(g.action, GroupAction::Failed))
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
/// `client` MUST point at a healthy admin endpoint of any cluster
/// member (preferably a leader, but the orchestrator follows
/// `TransferLeader` errors gracefully).
pub async fn decommission_node(
    client: &mut AdminClient,
    target_node_id: u64,
    dry_run: bool,
) -> Result<DecommissionReport> {
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
        let result =
            decommission_one_group(client, group_id, target_node_id, target_is_replica, dry_run)
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
            "groups {coverage_gaps:?} could not be probed from the contacted node although \
             the routing table lists node {target_node_id} as a replica — they were NOT \
             decommissioned; re-run against a node hosting them (or their leader)"
        ));
    }

    Ok(report)
}

async fn decommission_one_group(
    client: &mut AdminClient,
    group_id: u64,
    target_node_id: u64,
    target_is_replica: bool,
    dry_run: bool,
) -> Result<GroupOutcome> {
    let membership = match client.get_group_membership(GroupId { group_id }).await {
        Ok(resp) => resp.into_inner(),
        Err(status) if status.code() == Code::NotFound => {
            // The contacted node does not host this group.
            if target_is_replica {
                // The previously-silent skip: the routing table says the
                // target participates, but this group cannot be probed or
                // changed from here — report it as a loud failure.
                return Ok(GroupOutcome {
                    result: DecommissionGroupResult {
                        group_id,
                        action: GroupAction::Failed,
                        message: format!(
                            "contacted node does not host group {group_id}, but the routing \
                             table lists node {target_node_id} as a replica — NOT decommissioned; \
                             re-run against a node hosting this group (or its leader)"
                        ),
                    },
                    coverage_gap: true,
                });
            }
            return Ok(GroupOutcome::ok(DecommissionGroupResult {
                group_id,
                action: GroupAction::Skipped,
                message: "group not hosted on the contacted node and the routing table \
                          does not list the target as a replica"
                    .to_string(),
            }));
        }
        Err(status) => return Err(status.into()),
    };

    let voter_ids: Vec<u64> = membership.voters.iter().map(|m| m.node_id).collect();
    if !voter_ids.contains(&target_node_id) {
        return Ok(GroupOutcome::ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::Skipped,
            message: "target is not a voter in this group".to_string(),
        }));
    }

    let remaining: Vec<u64> = voter_ids
        .iter()
        .copied()
        .filter(|id| *id != target_node_id)
        .collect();
    // Use the shared predicate (not a bare threshold comparison) so this
    // pre-check cannot drift from the server-side guard if the policy ever
    // evolves beyond a single minimum (rr_core::membership module docs).
    if !membership_change_is_safe(remaining.len()) {
        return Ok(GroupOutcome::ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::AbortedQuorumLoss,
            message: format!(
                "removing target would leave {} voters (< {} minimum); add a new voter first",
                remaining.len(),
                MIN_VOTERS_AFTER_REMOVAL
            ),
        }));
    }

    if dry_run {
        return Ok(GroupOutcome::ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::WouldRemove,
            message: format!("dry-run: would change voters from {voter_ids:?} to {remaining:?}"),
        }));
    }

    let group_status = client
        .get_group_status(GroupId { group_id })
        .await?
        .into_inner();
    let mut drained_via_transfer = false;
    if target_is_leader(group_status.leader_node_id, target_node_id) {
        let new_leader = pick_new_leader(&membership.voters, target_node_id)
            .ok_or_else(|| anyhow!("no candidate voter to receive leadership"))?;
        let _ = client
            .transfer_leader(TransferLeaderRequest {
                group_id,
                target_node_id: new_leader,
            })
            .await?;
        drained_via_transfer = true;
        wait_for_drain(client, group_id, target_node_id).await?;
    }

    client
        .change_membership(ChangeMembershipRequest {
            group_id,
            voter_node_ids: remaining.clone(),
            // Decommission deliberately has no force channel: its contract
            // is "add a fresh voter first, then remove" (see module docs).
            force: false,
        })
        .await?;

    let confirm = client
        .get_group_membership(GroupId { group_id })
        .await?
        .into_inner();
    if confirm.voters.iter().any(|m| m.node_id == target_node_id) {
        return Ok(GroupOutcome::ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::Failed,
            message: "post-change membership still lists target as voter; re-run".to_string(),
        }));
    }

    Ok(GroupOutcome::ok(DecommissionGroupResult {
        group_id,
        action: if drained_via_transfer {
            GroupAction::DrainedAndRemoved
        } else {
            GroupAction::Removed
        },
        message: format!(
            "voters: {voter_ids:?} → {remaining:?}{}",
            if drained_via_transfer {
                " (after leadership transfer)"
            } else {
                ""
            }
        ),
    }))
}

fn pick_new_leader(voters: &[NodeMember], target_node_id: u64) -> Option<u64> {
    voters
        .iter()
        .find(|m| m.node_id != target_node_id)
        .map(|m| m.node_id)
}

async fn wait_for_drain(
    client: &mut AdminClient,
    group_id: u64,
    target_node_id: u64,
) -> Result<()> {
    let deadline = Instant::now() + DRAIN_TIMEOUT;
    while Instant::now() < deadline {
        let status = client
            .get_group_status(GroupId { group_id })
            .await?
            .into_inner();
        if status.leader_node_id != 0 && status.leader_node_id != target_node_id {
            return Ok(());
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
        assert!(!report.has_failures());
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

    #[test]
    fn pick_new_leader_picks_first_non_target_voter() {
        let voters = vec![
            NodeMember {
                node_id: 4,
                addr: String::new(),
            },
            NodeMember {
                node_id: 1,
                addr: String::new(),
            },
            NodeMember {
                node_id: 2,
                addr: String::new(),
            },
        ];
        assert_eq!(pick_new_leader(&voters, 4), Some(1));
    }

    #[test]
    fn pick_new_leader_returns_none_when_only_target() {
        let voters = vec![NodeMember {
            node_id: 4,
            addr: String::new(),
        }];
        assert_eq!(pick_new_leader(&voters, 4), None);
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
}

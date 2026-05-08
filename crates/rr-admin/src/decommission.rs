//! `rr-admin node decommission` — three-step orchestrator that drains
//! a target node from every Raft group it participates in and removes
//! it from the voter set.
//!
//! # Workflow
//!
//! For each group `G` reported by `NodeStatus.group_ids`:
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
//! 3. **Drain** if the target is the current leader: call
//!    `TransferLeader` to a remaining voter, then poll
//!    `GetGroupStatus` until `is_leader=false` for the target (up to
//!    [`DRAIN_TIMEOUT`]).
//! 4. **Remove** by calling `ChangeMembership(G, voters - {target})`.
//! 5. **Confirm** by re-querying membership and asserting the target
//!    is no longer a voter.
//!
//! Stops at the first failure and reports the partial-progress state
//! so the operator can resume (re-run is idempotent given the
//! framework's `change_membership` semantics).
//!
//! # Dry run
//!
//! `--dry-run` skips steps 3-5 and prints what would happen. Useful
//! for checking quorum impact before committing.

use anyhow::{anyhow, Result};
use rr_network::proto::admin::admin_service_client::AdminServiceClient;
use rr_network::proto::admin::{
    ChangeMembershipRequest, Empty, GroupId, NodeMember, TransferLeaderRequest,
};
use serde::Serialize;
use std::time::{Duration, Instant};
use tonic::transport::Channel;

/// Maximum time we wait for a transferred leadership to land on the
/// new leader before giving up on the drain step. ~10s covers a
/// healthy 3-5 node cluster + one election timeout.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often we poll `GetGroupStatus` while waiting for drain to
/// complete. Mirrors openraft's tick cadence.
pub const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Smallest voter-set size we'll allow `change_membership` to leave
/// behind. `< 2` voters means single-point-of-failure or a torn-down
/// group, both of which require an explicit operator action (add a
/// fresh voter first, or `node-reset` / cluster teardown).
pub const MIN_VOTERS_AFTER_REMOVAL: usize = 2;

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

/// Run the decommission workflow against `client` for `target_node_id`.
///
/// `client` MUST point at a healthy admin endpoint of any cluster
/// member (preferably a leader, but the orchestrator follows
/// `TransferLeader` errors gracefully).
pub async fn decommission_node(
    client: &mut AdminServiceClient<Channel>,
    target_node_id: u64,
    dry_run: bool,
) -> Result<DecommissionReport> {
    let node_status = client.get_node_status(Empty {}).await?.into_inner();
    let group_ids = node_status.group_ids;

    let mut report = DecommissionReport {
        target_node_id,
        dry_run,
        groups: Vec::with_capacity(group_ids.len()),
    };

    for group_id in group_ids {
        let result = decommission_one_group(client, group_id, target_node_id, dry_run).await;
        report.groups.push(match result {
            Ok(r) => r,
            Err(e) => DecommissionGroupResult {
                group_id,
                action: GroupAction::Failed,
                message: format!("orchestrator error: {e}"),
            },
        });
    }

    Ok(report)
}

async fn decommission_one_group(
    client: &mut AdminServiceClient<Channel>,
    group_id: u64,
    target_node_id: u64,
    dry_run: bool,
) -> Result<DecommissionGroupResult> {
    let membership = client
        .get_group_membership(GroupId { group_id })
        .await?
        .into_inner();

    let voter_ids: Vec<u64> = membership.voters.iter().map(|m| m.node_id).collect();
    if !voter_ids.contains(&target_node_id) {
        return Ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::Skipped,
            message: "target is not a voter in this group".to_string(),
        });
    }

    let remaining: Vec<u64> = voter_ids
        .iter()
        .copied()
        .filter(|id| *id != target_node_id)
        .collect();
    if remaining.len() < MIN_VOTERS_AFTER_REMOVAL {
        return Ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::AbortedQuorumLoss,
            message: format!(
                "removing target would leave {} voters (< {} minimum); add a new voter first",
                remaining.len(),
                MIN_VOTERS_AFTER_REMOVAL
            ),
        });
    }

    if dry_run {
        return Ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::WouldRemove,
            message: format!("dry-run: would change voters from {voter_ids:?} to {remaining:?}"),
        });
    }

    let group_status = client
        .get_group_status(GroupId { group_id })
        .await?
        .into_inner();
    let mut drained_via_transfer = false;
    if group_status.is_leader && group_status.leader_node_id == target_node_id {
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

    let resp = client
        .change_membership(ChangeMembershipRequest {
            group_id,
            voter_node_ids: remaining.clone(),
        })
        .await?
        .into_inner();
    if !resp.success {
        return Ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::Failed,
            message: format!("change_membership failed: {}", resp.message),
        });
    }

    let confirm = client
        .get_group_membership(GroupId { group_id })
        .await?
        .into_inner();
    if confirm.voters.iter().any(|m| m.node_id == target_node_id) {
        return Ok(DecommissionGroupResult {
            group_id,
            action: GroupAction::Failed,
            message: "post-change membership still lists target as voter; re-run".to_string(),
        });
    }

    Ok(DecommissionGroupResult {
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
    })
}

fn pick_new_leader(voters: &[NodeMember], target_node_id: u64) -> Option<u64> {
    voters
        .iter()
        .find(|m| m.node_id != target_node_id)
        .map(|m| m.node_id)
}

async fn wait_for_drain(
    client: &mut AdminServiceClient<Channel>,
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
        };
        assert!(!report.made_progress());
    }

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

    #[test]
    fn min_voters_after_removal_constant_is_two() {
        assert_eq!(MIN_VOTERS_AFTER_REMOVAL, 2);
    }
}

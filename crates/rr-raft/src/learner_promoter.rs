//! Per-group background task that promotes caught-up learners to
//! voters automatically.
//!
//! # Lifecycle
//!
//! ```text
//!  ┌─────────────┐  becomes Leader   ┌────────────────────────────┐
//!  │  Following  │ ────────────────▶│ Polling: per-tick, scan    │
//!  │  (parked)   │ ◀────────────────│ membership for learners,   │
//!  └─────────────┘  steps down       │ track stability windows    │
//!                                   └────────────────────────────┘
//!                                              │
//!                              learner stable >= stability_ticks
//!                                              ▼
//!                              ┌──────────────────────────────────┐
//!                              │ call promote_learners(learners)  │
//!                              │  → audit-log promotion           │
//!                              └──────────────────────────────────┘
//! ```
//!
//! # Stability window
//!
//! "Caught up" is defined as `last_log_index - matched_index <=
//! max_lag`. To avoid promoting a learner that's about to fall
//! behind again (e.g. transient network blip), the promoter waits
//! for `stability_ticks` consecutive successful evaluations before
//! triggering the promotion. With the default 5s tick + 3 stability
//! ticks, promotion takes ~15s of sustained caught-up state.
//!
//! # Why this lives in rr-raft, not rr-server
//!
//! Promotion calls `RaftGroupManager::promote_learners` which is a
//! purely-internal openraft operation; no gRPC boundary needed. The
//! audit emission goes via `tracing` so operators see promotions in
//! the existing `rr::audit` stream alongside manual changes from
//! `rr-admin group change-membership`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::type_config::alias::WatchReceiverOf;
use openraft::type_config::async_runtime::WatchReceiver;
use openraft::ServerState;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use rr_core::types::{GroupId, NodeId};

use crate::group_manager::RaftGroupManager;
use crate::type_config::RrTypeConfig;

type RrRaftMetrics = openraft::RaftMetrics<RrTypeConfig>;

/// `tracing` target for audit events; mirrors `rr_network::audit::AUDIT_TARGET`.
/// Inlined here because rr-raft cannot depend on rr-network (would
/// create a dependency cycle).
const AUDIT_TARGET: &str = "rr::audit";

/// Default poll interval. Aligned with the Prometheus-friendly 5s
/// cadence the metrics ticker uses.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Default `max_lag` — number of log entries the learner is allowed
/// to be behind the leader's `last_log_index` while still counting as
/// "caught up". 50 is conservative for typical write throughput; tune
/// up for high-throughput workloads to avoid never-promoting.
pub const DEFAULT_MAX_LAG: u64 = 50;

/// Default `stability_ticks` — how many consecutive caught-up polls
/// are required before promoting. With the default poll interval this
/// gives a ~15-second debounce.
pub const DEFAULT_STABILITY_TICKS: u32 = 3;

/// Behaviour knobs for [`spawn_for_group`].
#[derive(Clone, Debug)]
pub struct AutoPromoteConfig {
    pub max_lag: u64,
    pub stability_ticks: u32,
    pub poll_interval: Duration,
}

impl Default for AutoPromoteConfig {
    fn default() -> Self {
        Self {
            max_lag: DEFAULT_MAX_LAG,
            stability_ticks: DEFAULT_STABILITY_TICKS,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }
}

/// Handle returned by [`spawn_for_group`]. Drop or
/// [`Self::stop`] to terminate the promoter.
pub struct LearnerPromoterHandle {
    join: JoinHandle<()>,
    cancel: watch::Sender<bool>,
}

impl LearnerPromoterHandle {
    /// Cancel the promoter and await its exit.
    pub async fn stop(self) {
        let _ = self.cancel.send(true);
        let _ = self.join.await;
    }
}

/// Spawn a per-group learner-promoter task. The task runs only while
/// this node is the leader of `group_id`; on follower transitions it
/// parks until leadership returns.
pub fn spawn_for_group(
    mgr: Arc<RaftGroupManager>,
    group_id: GroupId,
    config: AutoPromoteConfig,
) -> LearnerPromoterHandle {
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let join = tokio::spawn(async move {
        if let Err(e) = run(mgr, group_id, config, cancel_rx).await {
            warn!(group_id, error = %e, "learner-promoter exited with error");
        }
    });
    LearnerPromoterHandle {
        join,
        cancel: cancel_tx,
    }
}

async fn run(
    mgr: Arc<RaftGroupManager>,
    group_id: GroupId,
    config: AutoPromoteConfig,
    mut cancel_rx: watch::Receiver<bool>,
) -> Result<(), String> {
    let raft = mgr
        .get_raft(group_id)
        .await
        .ok_or_else(|| format!("learner-promoter: group {group_id} not started"))?;
    let metrics_rx = raft.metrics();
    let mut tick = tokio::time::interval(config.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stable_ticks: BTreeMap<NodeId, u32> = BTreeMap::new();
    info!(
        group_id,
        max_lag = config.max_lag,
        stability_ticks = config.stability_ticks,
        poll_interval_ms = config.poll_interval.as_millis() as u64,
        "learner-promoter started"
    );

    loop {
        tokio::select! {
            _ = cancel_rx.changed() => {
                debug!(group_id, "learner-promoter cancelled");
                return Ok(());
            }
            _ = tick.tick() => {
                let snapshot: RrRaftMetrics = metrics_rx.borrow_watched().clone();
                if !matches!(snapshot.state, ServerState::Leader) {
                    if !stable_ticks.is_empty() {
                        debug!(group_id, "stepped down; clearing learner stability counters");
                        stable_ticks.clear();
                    }
                    continue;
                }
                evaluate_and_promote(
                    &mgr,
                    group_id,
                    &metrics_rx,
                    &snapshot,
                    &config,
                    &mut stable_ticks,
                )
                .await;
            }
        }
    }
}

async fn evaluate_and_promote(
    mgr: &RaftGroupManager,
    group_id: GroupId,
    metrics_rx: &WatchReceiverOf<RrTypeConfig, RrRaftMetrics>,
    metrics: &RrRaftMetrics,
    config: &AutoPromoteConfig,
    stable_ticks: &mut BTreeMap<NodeId, u32>,
) {
    let cfg = metrics.membership_config.membership();
    let learner_ids: BTreeSet<NodeId> = cfg.learner_ids().collect();
    if learner_ids.is_empty() {
        if !stable_ticks.is_empty() {
            stable_ticks.clear();
        }
        return;
    }

    let leader_index = metrics.last_log_index.unwrap_or(0);
    let Some(replication) = metrics.replication.as_ref() else {
        return;
    };

    stable_ticks.retain(|nid, _| learner_ids.contains(nid));

    // Every learner crossing the stability threshold THIS tick goes into ONE
    // incremental promotion call so the batch is audited and retried together.
    //
    // A learner with NO replication record (`None`) is not "replicated up to
    // index 0": the leader has never reported progress for it, so it must
    // not count as caught up.
    let promotable = plan_promotions(
        group_id,
        &learner_ids,
        |nid| {
            replication
                .get(nid)
                .and_then(|opt| opt.as_ref())
                .map(|l| l.index)
        },
        leader_index,
        config,
        stable_ticks,
    );
    if promotable.is_empty() {
        return;
    }

    // Read membership LIVE just before the call: a planned learner that is no
    // longer present aborts the attempt (re-planned next tick). Incremental
    // promotion leaves the current voter set untouched.
    let live_membership = metrics_rx
        .borrow_watched()
        .membership_config
        .membership()
        .clone();
    let live_learners: BTreeSet<NodeId> = live_membership.learner_ids().collect();
    if !promotable.iter().all(|nid| live_learners.contains(nid)) {
        warn!(
            target: AUDIT_TARGET,
            op = "AutoPromoteLearner",
            group_id,
            planned_node_ids = ?promotable,
            success = false,
            result = "aborted",
            "learner auto-promotion aborted: membership changed since planning tick"
        );
        return;
    }
    info!(
        target: AUDIT_TARGET,
        op = "AutoPromoteLearner",
        group_id,
        promoted_node_ids = ?promotable,
        new_voter_count = live_membership.voter_ids().count() + promotable.len(),
        result = "started",
        "auto-promoting caught-up learners"
    );
    metrics::counter!(
        "rr_learner_promotions_total",
        "group_id" => group_id.to_string()
    )
    .increment(promotable.len() as u64);
    match mgr
        .promote_learners(group_id, promotable.iter().copied().collect())
        .await
    {
        Ok(()) => {
            info!(
                target: AUDIT_TARGET,
                op = "AutoPromoteLearner",
                group_id,
                promoted_node_ids = ?promotable,
                success = true,
                result = "succeeded",
                "learners auto-promoted"
            );
            for nid in &promotable {
                stable_ticks.remove(nid);
            }
        }
        Err(e) => {
            warn!(
                target: AUDIT_TARGET,
                op = "AutoPromoteLearner",
                group_id,
                promoted_node_ids = ?promotable,
                success = false,
                result = "failed",
                error = %e,
                "learner auto-promotion failed; will retry next tick"
            );
            metrics::counter!(
                "rr_learner_promotion_failures_total",
                "group_id" => group_id.to_string()
            )
            .increment(1);
        }
    }
}

/// Advance every caught-up learner's stability counter and return the ids
/// that reached `config.stability_ticks` on this tick. Pure planning, split
/// from [`evaluate_and_promote`] so the batching rule (one incremental
/// promotion per tick) is unit-testable without a live cluster.
///
/// `matched_index` returns `None` for a learner with no replication record.
/// `None` is NOT index 0: the leader has never reported progress for that
/// learner, so it is treated as not caught up (its stability chain resets)
/// rather than as matched-at-index-0 — on a young group whose
/// `last_log_index <= max_lag`, reading `None` as 0 would count a learner
/// that never replicated as caught up.
fn plan_promotions(
    group_id: GroupId,
    learner_ids: &BTreeSet<NodeId>,
    matched_index: impl Fn(&NodeId) -> Option<u64>,
    leader_index: u64,
    config: &AutoPromoteConfig,
    stable_ticks: &mut BTreeMap<NodeId, u32>,
) -> Vec<NodeId> {
    let mut promotable = Vec::new();
    for nid in learner_ids {
        let Some(matched) = matched_index(nid) else {
            if let Some(entry) = stable_ticks.get_mut(nid) {
                if *entry > 0 {
                    debug!(
                        group_id,
                        follower_node_id = nid,
                        "no replication record; resetting stability counter"
                    );
                    *entry = 0;
                }
            }
            continue;
        };
        let lag = leader_index.saturating_sub(matched);
        if lag <= config.max_lag {
            let entry = stable_ticks.entry(*nid).or_insert(0);
            *entry += 1;
            debug!(
                group_id,
                follower_node_id = nid,
                lag,
                stable_ticks = *entry,
                threshold = config.stability_ticks,
                "learner caught-up tick"
            );
            if *entry >= config.stability_ticks {
                promotable.push(*nid);
            }
        } else if let Some(entry) = stable_ticks.get_mut(nid) {
            if *entry > 0 {
                debug!(
                    group_id,
                    follower_node_id = nid,
                    lag,
                    "learner fell behind; resetting stability counter"
                );
                *entry = 0;
            }
        }
    }
    promotable
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::group_manager::{RaftGroupManager, StateMachineFactory};
    use openraft::BasicNode;
    use rr_core::error::RrError;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;

    struct NoopSM;

    #[async_trait::async_trait]
    impl StateMachine for NoopSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            Ok(entry.to_vec())
        }
        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }
        async fn restore(&mut self, _: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }

    fn noop_factory() -> StateMachineFactory {
        Box::new(|_gid| Box::new(NoopSM))
    }

    #[tokio::test]
    async fn promoter_starts_and_stops_cleanly() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        mgr.start_group(1).await.unwrap();
        let mut members = BTreeMap::new();
        members.insert(
            1u64,
            BasicNode {
                addr: String::new(),
            },
        );
        mgr.init_cluster(1, members).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let cfg = AutoPromoteConfig {
            poll_interval: Duration::from_millis(50),
            ..Default::default()
        };
        let handle = spawn_for_group(Arc::clone(&mgr), 1, cfg);
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.stop().await;
        mgr.stop_group(1).await.unwrap();
    }

    #[tokio::test]
    async fn promoter_exits_when_group_unknown() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let cfg = AutoPromoteConfig {
            poll_interval: Duration::from_millis(50),
            ..Default::default()
        };
        let handle = spawn_for_group(Arc::clone(&mgr), 999, cfg);
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.stop().await;
    }

    #[tokio::test]
    async fn auto_promote_config_default_values_match_constants() {
        let cfg = AutoPromoteConfig::default();
        assert_eq!(cfg.max_lag, DEFAULT_MAX_LAG);
        assert_eq!(cfg.stability_ticks, DEFAULT_STABILITY_TICKS);
        assert_eq!(cfg.poll_interval, DEFAULT_POLL_INTERVAL);
    }

    /// Two learners crossing the stability threshold in the SAME tick must be
    /// collected into one plan so the tick is audited and retried as one batch.
    #[test]
    fn plan_promotions_batches_all_stable_learners_in_one_tick() {
        let cfg = AutoPromoteConfig {
            max_lag: 10,
            stability_ticks: 2,
            poll_interval: Duration::from_millis(50),
        };
        let learners = BTreeSet::from([2u64, 3u64]);
        let mut stable = BTreeMap::new();

        // Tick 1: both caught up but below the threshold.
        let p = plan_promotions(1, &learners, |_| Some(100), 100, &cfg, &mut stable);
        assert!(p.is_empty());
        assert_eq!(stable.get(&2), Some(&1));
        assert_eq!(stable.get(&3), Some(&1));

        // Tick 2: both cross the threshold together → ONE batched plan.
        let p = plan_promotions(1, &learners, |_| Some(100), 100, &cfg, &mut stable);
        assert_eq!(
            p,
            vec![2, 3],
            "all learners stable in one tick must be promoted together"
        );
    }

    /// A learner that falls behind resets its counter and is excluded from
    /// the batch; counters of removed learners are pruned by the caller.
    #[test]
    fn plan_promotions_resets_lagging_learners() {
        let cfg = AutoPromoteConfig {
            max_lag: 10,
            stability_ticks: 2,
            poll_interval: Duration::from_millis(50),
        };
        let learners = BTreeSet::from([2u64, 3u64]);
        let mut stable = BTreeMap::new();
        plan_promotions(1, &learners, |_| Some(100), 100, &cfg, &mut stable);

        // Node 3 falls 50 behind (lag > max_lag): counter resets, only node 2
        // crosses the threshold.
        let matched = |nid: &u64| if *nid == 3 { Some(50) } else { Some(100) };
        let p = plan_promotions(1, &learners, matched, 100, &cfg, &mut stable);
        assert_eq!(p, vec![2]);
        assert_eq!(stable.get(&3), Some(&0));
    }

    /// G4(b) regression: a learner with NO replication record (`None`) must
    /// never count as caught up — on a young group (last_log_index <=
    /// max_lag) the old `unwrap_or(0)` would treat "never replicated" as
    /// "replicated up to index 0" and promote it.
    #[test]
    fn plan_promotions_skips_learners_without_replication_record() {
        let cfg = AutoPromoteConfig {
            max_lag: 10,
            stability_ticks: 2,
            poll_interval: Duration::from_millis(50),
        };
        let learners = BTreeSet::from([2u64]);
        let mut stable = BTreeMap::new();

        // Young log: leader_index 5 <= max_lag 10, so a bogus matched=0
        // default WOULD qualify. None must not accumulate any stability.
        for _ in 0..3 {
            let p = plan_promotions(1, &learners, |_| None, 5, &cfg, &mut stable);
            assert!(
                p.is_empty(),
                "learner without a replication record must never be promotable"
            );
        }
        assert!(
            stable.get(&2).is_none_or(|v| *v == 0),
            "no stability credit without a replication record"
        );

        // Contrast: an explicit record AT index 0 on the same young log is a
        // real replication report and does count (lag 5 <= max_lag).
        plan_promotions(1, &learners, |_| Some(0), 5, &cfg, &mut stable);
        assert_eq!(stable.get(&2), Some(&1));
        let p = plan_promotions(1, &learners, |_| Some(0), 5, &cfg, &mut stable);
        assert_eq!(p, vec![2]);
    }

    /// G4(b): a learner whose replication record DISAPPEARS after earning
    /// stability credit loses it — the consecutive-caught-up chain is broken.
    #[test]
    fn plan_promotions_resets_counter_when_record_disappears() {
        let cfg = AutoPromoteConfig {
            max_lag: 10,
            stability_ticks: 3,
            poll_interval: Duration::from_millis(50),
        };
        let learners = BTreeSet::from([2u64]);
        let mut stable = BTreeMap::new();
        plan_promotions(1, &learners, |_| Some(100), 100, &cfg, &mut stable);
        assert_eq!(stable.get(&2), Some(&1));

        let p = plan_promotions(1, &learners, |_| None, 100, &cfg, &mut stable);
        assert!(p.is_empty());
        assert_eq!(stable.get(&2), Some(&0));
    }
}

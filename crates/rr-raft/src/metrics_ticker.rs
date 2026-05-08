//! Per-group periodic emitter for openraft-derived metrics that are
//! only computable on the leader and would otherwise be invisible
//! between writes.
//!
//! Specifically: `rr_raft_replication_lag{group_id, follower_node_id}`
//! gauges, which require reading the leader's `RaftMetrics.replication`
//! map and computing `last_log_index - matched.index` per follower.
//! [`crate::group_manager::RaftGroupManager::write`] already emits
//! `rr_raft_is_leader` / `rr_raft_current_term` / `rr_raft_last_log_index`
//! on every write; the ticker fills the gap between writes and adds
//! the per-follower lag dimension.
//!
//! On follower nodes the gauge is left absent (we explicitly do not
//! emit `0` because Prometheus would treat that as "lag is 0" rather
//! than "we are not a leader and have no data"). Use
//! `rr_raft_is_leader{group_id} == 1` as the predicate when alerting
//! on lag.

use std::sync::Arc;
use std::time::Duration;

use openraft::type_config::async_runtime::WatchReceiver;
use openraft::ServerState;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use rr_core::types::GroupId;

use crate::group_manager::RaftGroupManager;
use crate::type_config::RrTypeConfig;

type RrRaftMetrics = openraft::RaftMetrics<RrTypeConfig>;

/// How often the ticker reads metrics and emits the replication lag
/// gauge. 5 seconds matches Prometheus's default scrape interval —
/// scraping more often than this would just see the same value twice.
pub const METRICS_TICK_INTERVAL: Duration = Duration::from_secs(5);

/// Handle returned by [`spawn_for_group`]. Drop this (or call
/// [`Self::stop`]) to terminate the ticker.
pub struct MetricsTickerHandle {
    join: JoinHandle<()>,
    cancel: watch::Sender<bool>,
}

impl MetricsTickerHandle {
    /// Cancel the ticker and await its exit.
    pub async fn stop(self) {
        let _ = self.cancel.send(true);
        let _ = self.join.await;
    }
}

/// Spawn a per-group metrics ticker. The ticker exits cleanly when
/// the group's `Raft` instance is dropped (the `metrics()` watch
/// channel closes) or when [`MetricsTickerHandle::stop`] is called.
pub fn spawn_for_group(mgr: Arc<RaftGroupManager>, group_id: GroupId) -> MetricsTickerHandle {
    spawn_for_group_with_interval(mgr, group_id, METRICS_TICK_INTERVAL)
}

/// Variant accepting an explicit interval — used by tests to drive
/// the ticker faster than the production 5 seconds.
pub fn spawn_for_group_with_interval(
    mgr: Arc<RaftGroupManager>,
    group_id: GroupId,
    interval: Duration,
) -> MetricsTickerHandle {
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let join = tokio::spawn(async move {
        if let Err(e) = run(mgr, group_id, interval, cancel_rx).await {
            warn!(group_id, error = %e, "metrics ticker exited with error");
        }
    });
    MetricsTickerHandle {
        join,
        cancel: cancel_tx,
    }
}

async fn run(
    mgr: Arc<RaftGroupManager>,
    group_id: GroupId,
    interval: Duration,
    mut cancel_rx: watch::Receiver<bool>,
) -> Result<(), String> {
    let raft = mgr
        .get_raft(group_id)
        .await
        .ok_or_else(|| format!("metrics ticker: group {group_id} not started"))?;
    let mut metrics_rx = raft.metrics();
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut was_leader = false;
    let initial: RrRaftMetrics = metrics_rx.borrow_watched().clone();
    emit(group_id, &initial);
    was_leader = maybe_notify_leadership_transition(&mgr, group_id, was_leader, &initial).await;

    loop {
        tokio::select! {
            _ = cancel_rx.changed() => {
                debug!(group_id, "metrics ticker cancelled");
                return Ok(());
            }
            changed = metrics_rx.changed() => {
                if changed.is_err() {
                    if was_leader {
                        mgr.notify_step_down(group_id).await;
                    }
                    debug!(group_id, "metrics ticker: raft metrics channel closed");
                    return Ok(());
                }

                let snapshot: RrRaftMetrics = metrics_rx.borrow_watched().clone();
                was_leader = maybe_notify_leadership_transition(&mgr, group_id, was_leader, &snapshot).await;
            }
            _ = tick.tick() => {
                let snapshot: RrRaftMetrics = metrics_rx.borrow_watched().clone();
                was_leader = maybe_notify_leadership_transition(&mgr, group_id, was_leader, &snapshot).await;
                emit(group_id, &snapshot);
            }
        }
    }
}

async fn maybe_notify_leadership_transition(
    mgr: &RaftGroupManager,
    group_id: GroupId,
    was_leader: bool,
    snapshot: &RrRaftMetrics,
) -> bool {
    let is_leader = matches!(snapshot.state, ServerState::Leader);
    match (was_leader, is_leader) {
        (false, true) => mgr.notify_leader_elected(group_id).await,
        (true, false) => mgr.notify_step_down(group_id).await,
        _ => {}
    }
    is_leader
}

fn emit(group_id: GroupId, m: &RrRaftMetrics) {
    let group_label = group_id.to_string();
    metrics::gauge!("rr_raft_current_term", "group_id" => group_label.clone())
        .set(m.current_term as f64);
    metrics::gauge!("rr_raft_last_log_index", "group_id" => group_label.clone())
        .set(m.last_log_index.unwrap_or(0) as f64);
    metrics::gauge!("rr_raft_is_leader", "group_id" => group_label.clone()).set(
        if matches!(m.state, ServerState::Leader) {
            1.0
        } else {
            0.0
        },
    );

    if !matches!(m.state, ServerState::Leader) {
        return;
    }

    let leader_index = m.last_log_index.unwrap_or(0);
    let Some(replication) = m.replication.as_ref() else {
        return;
    };
    for (follower_id, matched_opt) in replication.iter() {
        if *follower_id == m.id {
            continue;
        }
        let matched_index = matched_opt.as_ref().map(|l| l.index).unwrap_or(0);
        let lag = leader_index.saturating_sub(matched_index);
        metrics::gauge!(
            "rr_raft_replication_lag",
            "group_id" => group_label.clone(),
            "follower_node_id" => follower_id.to_string()
        )
        .set(lag as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::BasicNode;
    use std::collections::BTreeMap;

    use crate::group_manager::{RaftGroupManager, StateMachineFactory};
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
    async fn ticker_starts_and_stops_cleanly_on_a_running_group() {
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

        let handle = spawn_for_group_with_interval(Arc::clone(&mgr), 1, Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.stop().await;
        mgr.stop_group(1).await.unwrap();
    }

    #[tokio::test]
    async fn ticker_exits_when_group_unknown() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let handle =
            spawn_for_group_with_interval(Arc::clone(&mgr), 999, Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.stop().await;
    }
}

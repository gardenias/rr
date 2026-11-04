use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::type_config::async_runtime::WatchReceiver as _;
use openraft::BasicNode;
use rr_core::error::{LifecycleError, RrError};
use rr_core::storage::StorageEngine;
use rr_core::types::{GroupId, NodeId};
use rr_core::{
    CasOutcome, ClusterLifecycle, ClusterOrigin, FormationState, GroupFormation, LifecycleStore,
    SeedSpec, VersionedLifecycle,
};
use rr_raft::group_manager::{InitializeOutcome, RaftGroupManager};
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

use crate::health::ReadinessFlag;
use crate::identity::{self, LocalClusterIdentity};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormationMode {
    Genesis,
    AdoptLegacy,
}

pub struct FormationConfig {
    pub seed: SeedSpec,
    pub store: Arc<dyn LifecycleStore>,
    pub mode: FormationMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupFormationOutcome {
    Initialized,
    Recovered,
    AwaitingReplication,
    AdoptedExisting,
}

pub(crate) struct FormationSupervisor {
    node_id: NodeId,
    storage: Arc<dyn StorageEngine>,
    group_manager: Arc<RaftGroupManager>,
    config: FormationConfig,
    readiness: ReadinessFlag,
    reconcile_lock: Mutex<()>,
    /// Interned `group_id` metric labels, one per seed group: leaking a few
    /// bytes once per group avoids allocating a `String` per gauge update
    /// (same convention as `ClientDispatchImpl::group_label`).
    group_labels: BTreeMap<GroupId, &'static str>,
}

#[derive(Debug)]
pub(crate) struct FormationReport {
    pub outcomes: BTreeMap<GroupId, GroupFormationOutcome>,
    pub store_degraded: bool,
    all_formed: bool,
    leaders: BTreeMap<GroupId, Option<NodeId>>,
}

struct LocalGroup {
    initialized: bool,
    voters: BTreeSet<NodeId>,
    nodes: BTreeMap<NodeId, String>,
    leader: Option<NodeId>,
}

impl FormationSupervisor {
    pub fn new(
        node_id: NodeId,
        storage: Arc<dyn StorageEngine>,
        group_manager: Arc<RaftGroupManager>,
        config: FormationConfig,
        readiness: ReadinessFlag,
    ) -> Self {
        let group_labels = config
            .seed
            .groups
            .keys()
            .map(|group_id| {
                let label: &'static str = Box::leak(group_id.to_string().into_boxed_str());
                (*group_id, label)
            })
            .collect();
        Self {
            node_id,
            storage,
            group_manager,
            config,
            readiness,
            reconcile_lock: Mutex::new(()),
            group_labels,
        }
    }

    pub fn hosted_groups(&self) -> impl Iterator<Item = GroupId> + '_ {
        self.config.seed.groups.keys().copied()
    }

    pub fn lifecycle_store(&self) -> Arc<dyn LifecycleStore> {
        Arc::clone(&self.config.store)
    }

    pub async fn reconcile(&self) -> Result<FormationReport, RrError> {
        let _guard = self.reconcile_lock.lock().await;
        let start = std::time::Instant::now();
        let result = self.reconcile_inner().await;
        metrics::histogram!("rr_formation_duration_seconds").record(start.elapsed().as_secs_f64());
        match &result {
            Ok(report) => self.publish_readiness(report),
            Err(error) => self.readiness.set_phase(false, "failed", error.to_string()),
        }
        result
    }

    async fn reconcile_inner(&self) -> Result<FormationReport, RrError> {
        self.config.seed.validate()?;
        let group_ids: Vec<GroupId> = self.hosted_groups().collect();
        let mut local = BTreeMap::new();
        for group_id in &group_ids {
            local.insert(*group_id, self.inspect_local(*group_id).await?);
        }

        let loaded = match self.config.store.load_linearizable().await {
            Ok(loaded) => loaded,
            Err(error) => {
                if local.values().all(|group| group.initialized) {
                    let outcomes = group_ids
                        .iter()
                        .map(|group_id| (*group_id, GroupFormationOutcome::Recovered))
                        .collect();
                    let leaders = local
                        .iter()
                        .map(|(group_id, group)| (*group_id, group.leader))
                        .collect();
                    return Ok(FormationReport {
                        outcomes,
                        store_degraded: true,
                        all_formed: false,
                        leaders,
                    });
                }
                return Err(error.into());
            }
        };

        let mut versioned = self.authorize_or_create(loaded, &local).await?;
        self.publish_formation_state_gauges(&versioned.record);
        identity::store(
            self.storage.as_ref(),
            &LocalClusterIdentity {
                version: identity::LOCAL_CLUSTER_IDENTITY_VERSION,
                cluster_uuid: versioned.record.cluster_uuid,
                node_id: self.node_id,
            },
        )
        .await?;

        let mut outcomes = BTreeMap::new();
        let mut all_formed = true;
        for group_id in group_ids {
            loop {
                self.validate_record(&versioned.record)?;
                let formation = *versioned.record.groups.get(&group_id).ok_or_else(|| {
                    invalid_formation(format!("lifecycle is missing group {group_id}"))
                })?;
                let current_local = self.inspect_local(group_id).await?;
                let mut activate = false;
                let outcome = match (self.config.mode, formation.state, current_local.initialized) {
                    (FormationMode::Genesis, FormationState::Forming, initialized) => {
                        let initialization = if !initialized {
                            self.group_manager
                                .initialize_for_formation(group_id, self.seed_members())
                                .await?
                        } else {
                            InitializeOutcome::AlreadyInitialized
                        };
                        let mut current_local = self.inspect_local(group_id).await?;
                        for _ in 0..100 {
                            if current_local.initialized {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            current_local = self.inspect_local(group_id).await?;
                        }
                        activate = true;
                        if initialization == InitializeOutcome::Initialized {
                            GroupFormationOutcome::Initialized
                        } else {
                            GroupFormationOutcome::Recovered
                        }
                    }
                    (FormationMode::Genesis, FormationState::Active, false) => {
                        all_formed = false;
                        GroupFormationOutcome::AwaitingReplication
                    }
                    (FormationMode::Genesis, FormationState::Active, true) => {
                        GroupFormationOutcome::Recovered
                    }
                    (FormationMode::AdoptLegacy, FormationState::Active, false) => {
                        all_formed = false;
                        GroupFormationOutcome::AwaitingReplication
                    }
                    (FormationMode::AdoptLegacy, FormationState::Active, true) => {
                        GroupFormationOutcome::AdoptedExisting
                    }
                    (FormationMode::AdoptLegacy, FormationState::Forming, _) => {
                        return Err(invalid_formation(
                            "adopted lifecycle record cannot remain Forming",
                        )
                        .into());
                    }
                };

                if activate {
                    let mut next = versioned.record.clone();
                    next.groups
                        .get_mut(&group_id)
                        .expect("validated group")
                        .state = FormationState::Active;
                    let activation = self
                        .group_manager
                        .with_membership_mutation_lock(group_id, || async {
                            let current_local = self.inspect_local(group_id).await?;
                            if !self.matches_seed(group_id, &current_local) {
                                return Err(invalid_formation(format!(
                                    "group {group_id} actual membership does not match the Genesis seed: voters={:?}, nodes={:?}",
                                    current_local.voters, current_local.nodes
                                ))
                                .into());
                            }
                            self.config
                                .store
                                .compare_and_swap(versioned.revision, next.clone())
                                .await
                                .map_err(RrError::from)
                        })
                        .await?;
                    record_cas_outcome(activation);
                    match activation {
                        CasOutcome::Applied => {
                            versioned.revision =
                                versioned.revision.checked_add(1).ok_or_else(|| {
                                    LifecycleError::CorruptData(
                                        "lifecycle revision overflow".to_string(),
                                    )
                                })?;
                            versioned.record = next;
                            let formation = versioned.record.groups[&group_id];
                            info!(
                                group_id,
                                bootstrap_epoch = formation.bootstrap_epoch,
                                revision = versioned.revision,
                                "formation: group transitioned Forming -> Active"
                            );
                            self.set_formation_state_gauge(group_id, formation.state);
                        }
                        CasOutcome::Conflict => {
                            warn!(
                                group_id,
                                revision = versioned.revision,
                                "formation: lifecycle activation CAS conflict; reloading and retrying"
                            );
                            versioned = self.load_required().await?;
                            // The winning record may already show groups
                            // Active; republish so the gauge tracks the peer's
                            // transition within this reconcile.
                            self.publish_formation_state_gauges(&versioned.record);
                            continue;
                        }
                    }
                }
                outcomes.insert(group_id, outcome);
                break;
            }
        }

        let mut leaders = BTreeMap::new();
        for group_id in self.hosted_groups() {
            leaders.insert(group_id, self.inspect_local(group_id).await?.leader);
        }
        Ok(FormationReport {
            outcomes,
            store_degraded: false,
            all_formed,
            leaders,
        })
    }

    async fn authorize_or_create(
        &self,
        loaded: Option<VersionedLifecycle>,
        local: &BTreeMap<GroupId, LocalGroup>,
    ) -> Result<VersionedLifecycle, RrError> {
        if let Some(versioned) = loaded {
            self.validate_record(&versioned.record)?;
            if self.config.mode == FormationMode::Genesis
                && local.values().any(|group| group.initialized)
                && identity::load(self.storage.as_ref()).await?.is_none()
            {
                return Err(invalid_formation(
                    "initialized local state requires a matching managed identity before Genesis recovery",
                )
                .into());
            }
            return Ok(versioned);
        }

        match self.config.mode {
            FormationMode::Genesis => {
                if local.values().any(|group| group.initialized) {
                    return Err(invalid_formation(
                        "Genesis with an initialized local group and no lifecycle record requires AdoptLegacy",
                    )
                    .into());
                }
            }
            FormationMode::AdoptLegacy => {
                if !local.values().all(|group| group.initialized) {
                    return Err(invalid_formation(
                        "AdoptLegacy requires all local groups to be initialized before cluster CAS",
                    )
                    .into());
                }
            }
        }

        let state = match self.config.mode {
            FormationMode::Genesis => FormationState::Forming,
            FormationMode::AdoptLegacy => FormationState::Active,
        };
        let origin = match self.config.mode {
            FormationMode::Genesis => ClusterOrigin::Genesis,
            FormationMode::AdoptLegacy => ClusterOrigin::LegacyAdopted,
        };
        let record = ClusterLifecycle {
            version: 1,
            cluster_uuid: Uuid::new_v4(),
            topology_revision: 1,
            seed_hash: self.config.seed.seed_hash()?,
            origin,
            groups: self
                .config
                .seed
                .groups
                .keys()
                .map(|group_id| {
                    (
                        *group_id,
                        GroupFormation {
                            bootstrap_epoch: 1,
                            state,
                        },
                    )
                })
                .collect(),
        };
        match self.config.mode {
            FormationMode::Genesis => info!(
                cluster_uuid = %record.cluster_uuid,
                groups = record.groups.len(),
                "formation: genesis started; creating cluster lifecycle record"
            ),
            FormationMode::AdoptLegacy => info!(
                cluster_uuid = %record.cluster_uuid,
                groups = record.groups.len(),
                "formation: legacy cluster adoption started; creating lifecycle record"
            ),
        }
        let cluster_uuid = record.cluster_uuid;
        let outcome = self.config.store.create_if_absent(record).await?;
        record_cas_outcome(outcome);
        if outcome == CasOutcome::Conflict {
            warn!(
                cluster_uuid = %cluster_uuid,
                "formation: lifecycle record create CAS conflict; loading the winning record"
            );
        }
        let versioned = self.load_required().await?;
        match self.config.mode {
            FormationMode::Genesis => info!(
                cluster_uuid = %versioned.record.cluster_uuid,
                revision = versioned.revision,
                "formation: genesis complete"
            ),
            FormationMode::AdoptLegacy => info!(
                cluster_uuid = %versioned.record.cluster_uuid,
                revision = versioned.revision,
                "formation: legacy cluster adoption complete"
            ),
        }
        Ok(versioned)
    }

    async fn load_required(&self) -> Result<VersionedLifecycle, RrError> {
        let versioned = self
            .config
            .store
            .load_linearizable()
            .await?
            .ok_or_else(|| {
                LifecycleError::CorruptData(
                    "lifecycle record disappeared during formation".to_string(),
                )
            })?;
        self.validate_record(&versioned.record)?;
        Ok(versioned)
    }

    fn validate_record(&self, record: &ClusterLifecycle) -> Result<(), RrError> {
        record.validate()?;
        if record.seed_hash != self.config.seed.seed_hash()? {
            return Err(LifecycleError::SeedMismatch.into());
        }
        let expected_origin = match self.config.mode {
            FormationMode::Genesis => ClusterOrigin::Genesis,
            FormationMode::AdoptLegacy => ClusterOrigin::LegacyAdopted,
        };
        if record.origin != expected_origin {
            return Err(invalid_formation("formation mode does not match lifecycle origin").into());
        }
        if !record.groups.keys().eq(self.config.seed.groups.keys()) {
            return Err(invalid_formation("lifecycle group set does not match the seed").into());
        }
        Ok(())
    }

    async fn inspect_local(&self, group_id: GroupId) -> Result<LocalGroup, RrError> {
        let raft =
            self.group_manager.get_raft(group_id).await.ok_or_else(|| {
                invalid_formation(format!("hosted group {group_id} is not started"))
            })?;
        let metrics = raft.metrics();
        let metrics = metrics.borrow_watched().clone();
        let membership = metrics.membership_config.membership();
        let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
        let nodes = membership
            .nodes()
            .map(|(node_id, node)| (*node_id, node.addr.clone()))
            .collect();
        Ok(LocalGroup {
            initialized: !voters.is_empty(),
            voters,
            nodes,
            leader: metrics.current_leader,
        })
    }

    fn matches_seed(&self, group_id: GroupId, local: &LocalGroup) -> bool {
        let expected_nodes: BTreeMap<NodeId, String> = self
            .config
            .seed
            .nodes
            .iter()
            .map(|(node_id, node)| (*node_id, node.addr.clone()))
            .collect();
        local.voters == self.config.seed.groups[&group_id].voters && local.nodes == expected_nodes
    }

    fn seed_members(&self) -> BTreeMap<NodeId, BasicNode> {
        self.config
            .seed
            .nodes
            .iter()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    BasicNode {
                        addr: node.addr.clone(),
                    },
                )
            })
            .collect()
    }

    /// Export every group's formation state as `rr_formation_state{group_id}`
    /// (0 = Forming, 1 = Active). Called on each reconcile right after the
    /// lifecycle record is loaded or created, so a restarted node republishes
    /// the stored state on startup.
    fn publish_formation_state_gauges(&self, record: &ClusterLifecycle) {
        for (group_id, formation) in &record.groups {
            self.set_formation_state_gauge(*group_id, formation.state);
        }
    }

    fn set_formation_state_gauge(&self, group_id: GroupId, state: FormationState) {
        let value = match state {
            FormationState::Forming => 0.0,
            FormationState::Active => 1.0,
        };
        // The record is validated against the seed before any gauge update,
        // so the interned label always exists.
        let label = self
            .group_labels
            .get(&group_id)
            .copied()
            .expect("group label interned for every seed group");
        metrics::gauge!("rr_formation_state", "group_id" => label).set(value);
    }

    fn publish_readiness(&self, report: &FormationReport) {
        if report.store_degraded {
            self.readiness.set_phase(
                false,
                "degraded",
                "lifecycle store unavailable; using initialized local state",
            );
        } else if report
            .outcomes
            .values()
            .any(|outcome| *outcome == GroupFormationOutcome::AwaitingReplication)
        {
            self.readiness.set_phase(
                false,
                "awaiting_replication",
                "a hosted group has no local membership",
            );
        } else if !report.all_formed {
            self.readiness
                .set_phase(false, "forming", "lifecycle activation is incomplete");
        } else if report.leaders.values().all(Option::is_some) {
            self.readiness.set(true);
        } else {
            self.readiness.set_phase(
                false,
                "waiting_for_leader",
                "every hosted group needs an explicit observed leader",
            );
        }
    }
}

fn invalid_formation(message: impl Into<String>) -> LifecycleError {
    LifecycleError::InvalidTransition(message.into())
}

/// Count one LifecycleStore CAS outcome as `rr_formation_cas_total{result}`.
/// Store errors are not counted: they propagate before an outcome exists.
fn record_cas_outcome(outcome: CasOutcome) {
    let result = match outcome {
        CasOutcome::Applied => "applied",
        CasOutcome::Conflict => "conflict",
    };
    metrics::counter!("rr_formation_cas_total", "result" => result).increment(1);
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use openraft::type_config::async_runtime::WatchReceiver as _;
    use openraft::BasicNode;
    use rr_config::MemLifecycleStore;
    use rr_core::error::{LifecycleError, RrError};
    use rr_core::state_machine::StateMachine;
    use rr_core::storage::StorageEngine;
    use rr_core::types::{LogIndex, NodeAddr, NodeId};
    use rr_core::{
        CasOutcome, ClusterLifecycle, ClusterOrigin, FormationState, GroupFormation,
        LifecycleStore, SeedSpec, VersionedLifecycle,
    };
    use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};
    use rr_storage::mem::MemStorage;
    use uuid::Uuid;

    use super::{FormationConfig, FormationMode, FormationSupervisor, GroupFormationOutcome};
    use crate::health::ReadinessFlag;
    use crate::identity::{self, LocalClusterIdentity};

    const CLUSTER_UUID: Uuid = Uuid::from_u128(0x1234);

    struct NoopSM;

    #[async_trait]
    impl StateMachine for NoopSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            Ok(entry.to_vec())
        }

        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }

        async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RrError> {
            Ok(())
        }

        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }

    fn noop_factory() -> StateMachineFactory {
        Box::new(|_group_id| Box::new(NoopSM))
    }

    fn nodes(ids: &[NodeId]) -> BTreeMap<NodeId, NodeAddr> {
        ids.iter()
            .map(|id| {
                (
                    *id,
                    NodeAddr {
                        addr: format!("node-{id}:9001"),
                    },
                )
            })
            .collect()
    }

    fn seed(ids: &[NodeId], group_count: u64) -> SeedSpec {
        SeedSpec::stock("formation-test", nodes(ids), group_count).unwrap()
    }

    fn lifecycle(
        seed: &SeedSpec,
        origin: ClusterOrigin,
        state: FormationState,
    ) -> ClusterLifecycle {
        ClusterLifecycle {
            version: 1,
            cluster_uuid: CLUSTER_UUID,
            topology_revision: 1,
            seed_hash: seed.seed_hash().unwrap(),
            origin,
            groups: seed
                .groups
                .keys()
                .map(|group_id| {
                    (
                        *group_id,
                        GroupFormation {
                            bootstrap_epoch: 1,
                            state,
                        },
                    )
                })
                .collect(),
        }
    }

    struct Fixture {
        supervisor: Arc<FormationSupervisor>,
        group_manager: Arc<RaftGroupManager>,
        storage: Arc<MemStorage>,
        seed: SeedSpec,
        readiness: ReadinessFlag,
    }

    async fn fixture(
        mode: FormationMode,
        node_id: NodeId,
        roster: &[NodeId],
        group_count: u64,
        store: Arc<dyn LifecycleStore>,
    ) -> Fixture {
        let seed = seed(roster, group_count);
        let group_manager = Arc::new(RaftGroupManager::new(node_id, noop_factory()));
        for group_id in seed.groups.keys() {
            group_manager.start_group(*group_id).await.unwrap();
        }
        let storage = Arc::new(MemStorage::new());
        let readiness = ReadinessFlag::new();
        let supervisor = Arc::new(FormationSupervisor::new(
            node_id,
            storage.clone() as Arc<dyn StorageEngine>,
            group_manager.clone(),
            FormationConfig {
                seed: seed.clone(),
                store,
                mode,
            },
            readiness.clone(),
        ));
        Fixture {
            supervisor,
            group_manager,
            storage,
            seed,
            readiness,
        }
    }

    async fn initialize_legacy(group_manager: &RaftGroupManager, group_id: u64, node_id: NodeId) {
        group_manager
            .init_cluster(
                group_id,
                BTreeMap::from([(
                    node_id,
                    BasicNode {
                        addr: "legacy-node:9001".to_string(),
                    },
                )]),
            )
            .await
            .unwrap();
    }

    async fn voters(group_manager: &RaftGroupManager, group_id: u64) -> BTreeSet<NodeId> {
        let raft = group_manager.get_raft(group_id).await.unwrap();
        let metrics = raft.metrics();
        let voters = metrics
            .borrow_watched()
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        voters
    }

    async fn actual_nodes(
        group_manager: &RaftGroupManager,
        group_id: u64,
    ) -> BTreeMap<NodeId, BasicNode> {
        let raft = group_manager.get_raft(group_id).await.unwrap();
        let metrics = raft.metrics();
        let nodes = metrics
            .borrow_watched()
            .membership_config
            .membership()
            .nodes()
            .map(|(id, node)| (*id, node.clone()))
            .collect();
        nodes
    }

    #[tokio::test]
    async fn genesis_all_pristine_creates_identity_initializes_and_activates() {
        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::Genesis, 1, &[1], 1, store.clone()).await;

        let report = fixture.supervisor.reconcile().await.unwrap();

        assert_eq!(report.outcomes[&1], GroupFormationOutcome::Initialized);
        assert_eq!(voters(&fixture.group_manager, 1).await, BTreeSet::from([1]));
        assert_eq!(
            actual_nodes(&fixture.group_manager, 1).await[&1].addr,
            "node-1:9001"
        );
        let current = store.load_linearizable().await.unwrap().unwrap();
        assert_eq!(current.record.origin, ClusterOrigin::Genesis);
        assert_eq!(current.record.groups[&1].state, FormationState::Active);
        let local = identity::load(fixture.storage.as_ref())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(local.cluster_uuid, current.record.cluster_uuid);
        assert_eq!(local.node_id, 1);
    }

    #[tokio::test]
    async fn genesis_any_initialized_without_record_requires_adoption() {
        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::Genesis, 1, &[1], 1, store.clone()).await;
        initialize_legacy(&fixture.group_manager, 1, 1).await;

        let error = fixture.supervisor.reconcile().await.unwrap_err();

        assert!(error.to_string().contains("AdoptLegacy"), "{error}");
        assert!(store.load_linearizable().await.unwrap().is_none());
        assert!(identity::load(fixture.storage.as_ref())
            .await
            .unwrap()
            .is_none());
        assert!(!fixture.readiness.is_ready());
    }

    #[tokio::test]
    async fn adoption_all_initialized_activates_without_expanding_legacy_membership() {
        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::AdoptLegacy, 1, &[1, 2, 3], 1, store.clone()).await;
        initialize_legacy(&fixture.group_manager, 1, 1).await;

        let report = fixture.supervisor.reconcile().await.unwrap();

        assert_eq!(report.outcomes[&1], GroupFormationOutcome::AdoptedExisting);
        assert_eq!(voters(&fixture.group_manager, 1).await, BTreeSet::from([1]));
        assert_eq!(
            actual_nodes(&fixture.group_manager, 1).await[&1].addr,
            "legacy-node:9001"
        );
        let current = store.load_linearizable().await.unwrap().unwrap();
        assert_eq!(current.record.origin, ClusterOrigin::LegacyAdopted);
        assert_eq!(current.record.groups[&1].state, FormationState::Active);
    }

    #[tokio::test]
    async fn adoption_mixed_local_state_fails_before_cluster_cas() {
        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::AdoptLegacy, 1, &[1], 2, store.clone()).await;
        initialize_legacy(&fixture.group_manager, 1, 1).await;

        let error = fixture.supervisor.reconcile().await.unwrap_err();

        assert!(error.to_string().contains("all local groups"), "{error}");
        assert!(store.load_linearizable().await.unwrap().is_none());
        assert!(voters(&fixture.group_manager, 2).await.is_empty());
        assert!(identity::load(fixture.storage.as_ref())
            .await
            .unwrap()
            .is_none());
    }

    struct CreateConflictStore {
        inner: MemLifecycleStore,
        first_load: AtomicBool,
    }

    #[async_trait]
    impl LifecycleStore for CreateConflictStore {
        async fn load_linearizable(&self) -> Result<Option<VersionedLifecycle>, LifecycleError> {
            if self.first_load.swap(false, Ordering::SeqCst) {
                Ok(None)
            } else {
                self.inner.load_linearizable().await
            }
        }

        async fn create_if_absent(
            &self,
            record: ClusterLifecycle,
        ) -> Result<CasOutcome, LifecycleError> {
            self.inner.create_if_absent(record).await
        }

        async fn compare_and_swap(
            &self,
            expected_revision: u64,
            record: ClusterLifecycle,
        ) -> Result<CasOutcome, LifecycleError> {
            self.inner.compare_and_swap(expected_revision, record).await
        }
    }

    struct BlockingActivationStore {
        inner: MemLifecycleStore,
        group_manager: std::sync::Mutex<Option<Arc<RaftGroupManager>>>,
        cas_started: tokio::sync::Notify,
        release_cas: tokio::sync::Notify,
        members_at_cas: std::sync::Mutex<Option<BTreeSet<NodeId>>>,
    }

    #[async_trait]
    impl LifecycleStore for BlockingActivationStore {
        async fn load_linearizable(&self) -> Result<Option<VersionedLifecycle>, LifecycleError> {
            self.inner.load_linearizable().await
        }

        async fn create_if_absent(
            &self,
            record: ClusterLifecycle,
        ) -> Result<CasOutcome, LifecycleError> {
            self.inner.create_if_absent(record).await
        }

        async fn compare_and_swap(
            &self,
            expected_revision: u64,
            record: ClusterLifecycle,
        ) -> Result<CasOutcome, LifecycleError> {
            let group_manager = self.group_manager.lock().unwrap().clone().unwrap();
            *self.members_at_cas.lock().unwrap() =
                Some(actual_nodes(&group_manager, 1).await.into_keys().collect());
            self.cas_started.notify_one();
            self.release_cas.notified().await;
            self.inner.compare_and_swap(expected_revision, record).await
        }
    }

    #[tokio::test]
    async fn forming_activation_cas_serializes_with_membership_mutation() {
        let store = Arc::new(BlockingActivationStore {
            inner: MemLifecycleStore::new(),
            group_manager: std::sync::Mutex::new(None),
            cas_started: tokio::sync::Notify::new(),
            release_cas: tokio::sync::Notify::new(),
            members_at_cas: std::sync::Mutex::new(None),
        });
        let fixture = fixture(FormationMode::Genesis, 1, &[1], 1, store.clone()).await;
        *store.group_manager.lock().unwrap() = Some(fixture.group_manager.clone());

        let reconciling = {
            let supervisor = fixture.supervisor.clone();
            tokio::spawn(async move { supervisor.reconcile().await })
        };
        store.cas_started.notified().await;

        let raft = fixture.group_manager.get_raft(1).await.unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while raft.metrics().borrow_watched().current_leader.is_none() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let mutation = {
            let group_manager = fixture.group_manager.clone();
            tokio::spawn(async move {
                group_manager
                    .add_learner_nonblocking(1, 2, "node-2:9001".to_string())
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !mutation.is_finished(),
            "membership mutation crossed the Forming-to-Active CAS"
        );

        store.release_cas.notify_one();
        reconciling.await.unwrap().unwrap();
        assert_eq!(
            store.members_at_cas.lock().unwrap().clone().unwrap(),
            BTreeSet::from([1])
        );
        mutation.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn genesis_create_cas_conflict_with_different_seed_fails_closed() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);

        let winner_seed = seed(&[1], 1);
        let mut winner = lifecycle(
            &winner_seed,
            ClusterOrigin::Genesis,
            FormationState::Forming,
        );
        winner.seed_hash = [0x55; 32];
        let inner = MemLifecycleStore::new();
        inner.create_if_absent(winner).await.unwrap();
        let store = Arc::new(CreateConflictStore {
            inner,
            first_load: AtomicBool::new(true),
        });
        let fixture = fixture(FormationMode::Genesis, 1, &[1], 1, store).await;

        let error = fixture.supervisor.reconcile().await.unwrap_err();

        assert!(matches!(
            error,
            RrError::Lifecycle(LifecycleError::SeedMismatch)
        ));
        assert!(voters(&fixture.group_manager, 1).await.is_empty());
        let rendered = handle.render();
        assert!(
            rendered.contains("rr_formation_cas_total{result=\"conflict\"} 1"),
            "losing the create race must count exactly one conflict: {rendered}"
        );
        assert!(
            !rendered.contains("rr_formation_cas_total{result=\"applied\"}"),
            "the losing node must not count any applied CAS: {rendered}"
        );
        assert!(
            rendered.contains("rr_formation_duration_seconds"),
            "reconcile wall duration histogram missing: {rendered}"
        );
    }

    #[tokio::test]
    async fn genesis_pristine_cas_winner_rejects_seed_matching_legacy_without_identity() {
        let store = Arc::new(MemLifecycleStore::new());
        let pristine = fixture(FormationMode::Genesis, 1, &[1], 1, store.clone()).await;
        pristine.supervisor.reconcile().await.unwrap();

        let legacy = fixture(FormationMode::Genesis, 1, &[1], 1, store.clone()).await;
        legacy
            .group_manager
            .init_cluster(1, legacy.supervisor.seed_members())
            .await
            .unwrap();

        let error = legacy.supervisor.reconcile().await.unwrap_err();

        assert!(error.to_string().contains("managed identity"), "{error}");
        assert_eq!(voters(&legacy.group_manager, 1).await, BTreeSet::from([1]));
        assert!(identity::load(legacy.storage.as_ref())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .load_linearizable()
                .await
                .unwrap()
                .unwrap()
                .record
                .groups[&1]
                .state,
            FormationState::Active
        );
    }

    #[tokio::test]
    async fn existing_record_rejects_mismatched_local_uuid_before_initialization() {
        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::Genesis, 1, &[1], 1, store.clone()).await;
        store
            .create_if_absent(lifecycle(
                &fixture.seed,
                ClusterOrigin::Genesis,
                FormationState::Forming,
            ))
            .await
            .unwrap();
        identity::store(
            fixture.storage.as_ref(),
            &LocalClusterIdentity {
                version: 1,
                cluster_uuid: Uuid::from_u128(0x5678),
                node_id: 1,
            },
        )
        .await
        .unwrap();

        let error = fixture.supervisor.reconcile().await.unwrap_err();

        assert!(matches!(
            error,
            RrError::Lifecycle(LifecycleError::IdentityMismatch)
        ));
        assert!(voters(&fixture.group_manager, 1).await.is_empty());
        assert_eq!(
            store
                .load_linearizable()
                .await
                .unwrap()
                .unwrap()
                .record
                .groups[&1]
                .state,
            FormationState::Forming
        );
    }

    #[tokio::test]
    async fn forming_genesis_initializes_full_roster_and_marks_active() {
        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::Genesis, 1, &[1, 2, 3], 1, store.clone()).await;
        store
            .create_if_absent(lifecycle(
                &fixture.seed,
                ClusterOrigin::Genesis,
                FormationState::Forming,
            ))
            .await
            .unwrap();

        let report = fixture.supervisor.reconcile().await.unwrap();

        assert_eq!(report.outcomes[&1], GroupFormationOutcome::Initialized);
        assert_eq!(
            voters(&fixture.group_manager, 1).await,
            BTreeSet::from([1, 2, 3])
        );
        let actual = actual_nodes(&fixture.group_manager, 1).await;
        for (node_id, node) in &fixture.seed.nodes {
            assert_eq!(actual[node_id].addr, node.addr);
        }
        assert_eq!(
            store
                .load_linearizable()
                .await
                .unwrap()
                .unwrap()
                .record
                .groups[&1]
                .state,
            FormationState::Active
        );
    }

    #[tokio::test]
    async fn active_genesis_with_pristine_local_group_awaits_replication() {
        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::Genesis, 1, &[1, 2, 3], 1, store.clone()).await;
        store
            .create_if_absent(lifecycle(
                &fixture.seed,
                ClusterOrigin::Genesis,
                FormationState::Active,
            ))
            .await
            .unwrap();

        let report = fixture.supervisor.reconcile().await.unwrap();

        assert_eq!(
            report.outcomes[&1],
            GroupFormationOutcome::AwaitingReplication
        );
        assert!(voters(&fixture.group_manager, 1).await.is_empty());
        assert!(!fixture.readiness.is_ready());
    }

    #[tokio::test]
    async fn active_initialized_recovery_allows_current_membership_to_differ_from_seed() {
        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::Genesis, 1, &[1, 2, 3], 1, store.clone()).await;
        store
            .create_if_absent(lifecycle(
                &fixture.seed,
                ClusterOrigin::Genesis,
                FormationState::Active,
            ))
            .await
            .unwrap();
        initialize_legacy(&fixture.group_manager, 1, 1).await;
        identity::store(
            fixture.storage.as_ref(),
            &LocalClusterIdentity {
                version: 1,
                cluster_uuid: CLUSTER_UUID,
                node_id: 1,
            },
        )
        .await
        .unwrap();

        let report = fixture.supervisor.reconcile().await.unwrap();

        assert_eq!(report.outcomes[&1], GroupFormationOutcome::Recovered);
        assert_eq!(voters(&fixture.group_manager, 1).await, BTreeSet::from([1]));
        assert_eq!(
            actual_nodes(&fixture.group_manager, 1).await[&1].addr,
            "legacy-node:9001"
        );
        let current = store.load_linearizable().await.unwrap().unwrap();
        assert_eq!(
            current.revision, 1,
            "Active recovery must not write a new lifecycle revision"
        );
        assert_eq!(current.record.groups[&1].state, FormationState::Active);
    }

    #[tokio::test]
    async fn late_initial_node_with_existing_identity_never_initializes_active_group() {
        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::Genesis, 3, &[1, 2, 3], 1, store.clone()).await;
        store
            .create_if_absent(lifecycle(
                &fixture.seed,
                ClusterOrigin::Genesis,
                FormationState::Active,
            ))
            .await
            .unwrap();
        identity::store(
            fixture.storage.as_ref(),
            &LocalClusterIdentity {
                version: 1,
                cluster_uuid: CLUSTER_UUID,
                node_id: 3,
            },
        )
        .await
        .unwrap();

        let report = fixture.supervisor.reconcile().await.unwrap();

        assert_eq!(
            report.outcomes[&1],
            GroupFormationOutcome::AwaitingReplication
        );
        assert!(voters(&fixture.group_manager, 1).await.is_empty());
        assert_eq!(
            identity::load(fixture.storage.as_ref())
                .await
                .unwrap()
                .unwrap()
                .node_id,
            3
        );
    }

    struct UnavailableStore;

    #[async_trait]
    impl LifecycleStore for UnavailableStore {
        async fn load_linearizable(&self) -> Result<Option<VersionedLifecycle>, LifecycleError> {
            Err(LifecycleError::Unavailable("injected outage".to_string()))
        }

        async fn create_if_absent(
            &self,
            _record: ClusterLifecycle,
        ) -> Result<CasOutcome, LifecycleError> {
            Err(LifecycleError::Unavailable("injected outage".to_string()))
        }

        async fn compare_and_swap(
            &self,
            _expected_revision: u64,
            _record: ClusterLifecycle,
        ) -> Result<CasOutcome, LifecycleError> {
            Err(LifecycleError::Unavailable("injected outage".to_string()))
        }
    }

    #[tokio::test]
    async fn store_outage_with_pristine_local_group_fails_closed() {
        let fixture = fixture(
            FormationMode::Genesis,
            1,
            &[1],
            1,
            Arc::new(UnavailableStore),
        )
        .await;

        let error = fixture.supervisor.reconcile().await.unwrap_err();

        assert!(matches!(
            error,
            RrError::Lifecycle(LifecycleError::Unavailable(_))
        ));
        assert!(voters(&fixture.group_manager, 1).await.is_empty());
        assert!(!fixture.readiness.is_ready());
    }

    #[tokio::test]
    async fn store_outage_with_initialized_group_recovers_as_degraded() {
        let fixture = fixture(
            FormationMode::Genesis,
            1,
            &[1],
            1,
            Arc::new(UnavailableStore),
        )
        .await;
        initialize_legacy(&fixture.group_manager, 1, 1).await;

        let report = fixture.supervisor.reconcile().await.unwrap();

        assert_eq!(report.outcomes[&1], GroupFormationOutcome::Recovered);
        assert!(report.store_degraded);
        assert_eq!(voters(&fixture.group_manager, 1).await, BTreeSet::from([1]));
        assert!(!fixture.readiness.is_ready());
    }

    #[tokio::test]
    async fn not_allowed_with_nonmatching_membership_does_not_become_active() {
        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::Genesis, 1, &[1, 2, 3], 1, store.clone()).await;
        store
            .create_if_absent(lifecycle(
                &fixture.seed,
                ClusterOrigin::Genesis,
                FormationState::Forming,
            ))
            .await
            .unwrap();
        initialize_legacy(&fixture.group_manager, 1, 1).await;
        identity::store(
            fixture.storage.as_ref(),
            &LocalClusterIdentity {
                version: 1,
                cluster_uuid: CLUSTER_UUID,
                node_id: 1,
            },
        )
        .await
        .unwrap();

        let error = fixture.supervisor.reconcile().await.unwrap_err();

        assert!(error.to_string().contains("membership"), "{error}");
        assert_eq!(voters(&fixture.group_manager, 1).await, BTreeSet::from([1]));
        assert_eq!(
            store
                .load_linearizable()
                .await
                .unwrap()
                .unwrap()
                .record
                .groups[&1]
                .state,
            FormationState::Forming
        );
        assert!(!fixture.readiness.is_ready());
    }

    #[tokio::test]
    async fn managed_node_starts_from_seed_and_waits_for_explicit_leader() {
        let provider = Arc::new(rr_config::MemConfigProvider::new());
        let config = crate::NodeConfig::new(
            1,
            "127.0.0.1:50051".to_string(),
            Arc::new(MemStorage::new()),
            provider,
            noop_factory(),
        );
        let node = crate::RrNode::new_with_formation(
            config,
            FormationConfig {
                seed: seed(&[1], 1),
                store: Arc::new(MemLifecycleStore::new()),
                mode: FormationMode::Genesis,
            },
        )
        .unwrap();

        node.start().await.unwrap();

        assert_eq!(voters(&node.group_manager(), 1).await, BTreeSet::from([1]));
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while !node.readiness().is_ready() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "leader readiness timed out"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn managed_hard_formation_failure_keeps_node_not_ready() {
        let group_manager = Arc::new(RaftGroupManager::new(1, noop_factory()));
        group_manager.start_group(1).await.unwrap();
        initialize_legacy(&group_manager, 1, 1).await;
        let mut config = crate::NodeConfig::new(
            1,
            "127.0.0.1:50051".to_string(),
            Arc::new(MemStorage::new()),
            Arc::new(rr_config::MemConfigProvider::new()),
            noop_factory(),
        );
        config.group_manager = Some(group_manager);
        let node = crate::RrNode::new_with_formation(
            config,
            FormationConfig {
                seed: seed(&[1], 1),
                store: Arc::new(MemLifecycleStore::new()),
                mode: FormationMode::Genesis,
            },
        )
        .unwrap();

        let error = node.start().await.unwrap_err();

        assert!(error.to_string().contains("AdoptLegacy"), "{error}");
        assert!(!node.readiness().is_ready());
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn managed_node_disables_legacy_node_added_membership_side_effects() {
        let provider = Arc::new(rr_config::MemConfigProvider::new());
        let config = crate::NodeConfig::new(
            1,
            "127.0.0.1:50051".to_string(),
            Arc::new(MemStorage::new()),
            provider.clone(),
            noop_factory(),
        );
        let node = crate::RrNode::new_with_formation(
            config,
            FormationConfig {
                seed: seed(&[1], 1),
                store: Arc::new(MemLifecycleStore::new()),
                mode: FormationMode::Genesis,
            },
        )
        .unwrap();
        node.start().await.unwrap();

        provider
            .add_node(
                2,
                NodeAddr {
                    addr: "node-2:9001".to_string(),
                },
            )
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(
            actual_nodes(&node.group_manager(), 1)
                .await
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([1])
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn genesis_reconcile_emits_formation_metrics() {
        // Thread-local recorder (documented for single-threaded runtimes):
        // deterministic capture regardless of any process-global recorder
        // installed by other tests in this binary.
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);

        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::Genesis, 1, &[1], 1, store.clone()).await;

        let report = fixture.supervisor.reconcile().await.unwrap();
        assert_eq!(report.outcomes[&1], GroupFormationOutcome::Initialized);

        let rendered = handle.render();
        // create_if_absent (genesis) + activation compare_and_swap both applied.
        assert!(
            rendered.contains("rr_formation_cas_total{result=\"applied\"} 2"),
            "genesis must count both CAS applications: {rendered}"
        );
        assert!(
            rendered.contains("rr_formation_state{group_id=\"1\"} 1"),
            "group 1 gauge must read Active (1) after genesis: {rendered}"
        );
        assert!(
            rendered.contains("rr_formation_duration_seconds"),
            "reconcile wall duration histogram missing: {rendered}"
        );
    }

    #[tokio::test]
    async fn adoption_reconcile_emits_formation_metrics() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);

        let store = Arc::new(MemLifecycleStore::new());
        let fixture = fixture(FormationMode::AdoptLegacy, 1, &[1, 2, 3], 1, store.clone()).await;
        initialize_legacy(&fixture.group_manager, 1, 1).await;

        let report = fixture.supervisor.reconcile().await.unwrap();
        assert_eq!(report.outcomes[&1], GroupFormationOutcome::AdoptedExisting);

        let rendered = handle.render();
        // Adoption creates the record already Active: one create CAS, no
        // activation CAS.
        assert!(
            rendered.contains("rr_formation_cas_total{result=\"applied\"} 1"),
            "adoption must count exactly the record create: {rendered}"
        );
        assert!(
            rendered.contains("rr_formation_state{group_id=\"1\"} 1"),
            "adopted group 1 gauge must read Active (1): {rendered}"
        );
        assert!(
            rendered.contains("rr_formation_duration_seconds"),
            "reconcile wall duration histogram missing: {rendered}"
        );
    }

    /// Store that plays the peer winning the activation race: the first
    /// compare_and_swap stores the incoming (Active) record but reports
    /// Conflict, so the reload observes the peer's transition.
    struct PeerWinsActivationStore {
        record: std::sync::Mutex<VersionedLifecycle>,
    }

    #[async_trait]
    impl LifecycleStore for PeerWinsActivationStore {
        async fn load_linearizable(&self) -> Result<Option<VersionedLifecycle>, LifecycleError> {
            Ok(Some(self.record.lock().unwrap().clone()))
        }

        async fn create_if_absent(
            &self,
            _record: ClusterLifecycle,
        ) -> Result<CasOutcome, LifecycleError> {
            Ok(CasOutcome::Conflict)
        }

        async fn compare_and_swap(
            &self,
            _expected_revision: u64,
            record: ClusterLifecycle,
        ) -> Result<CasOutcome, LifecycleError> {
            let mut current = self.record.lock().unwrap();
            current.revision += 1;
            current.record = record;
            Ok(CasOutcome::Conflict)
        }
    }

    #[tokio::test]
    async fn activation_cas_conflict_republishes_peer_active_state() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);

        let seed = seed(&[1], 1);
        let store = Arc::new(PeerWinsActivationStore {
            record: std::sync::Mutex::new(VersionedLifecycle {
                revision: 1,
                record: lifecycle(&seed, ClusterOrigin::Genesis, FormationState::Forming),
            }),
        });
        let fixture = fixture(FormationMode::Genesis, 1, &[1], 1, store.clone()).await;

        let report = fixture.supervisor.reconcile().await.unwrap();

        assert_eq!(report.outcomes[&1], GroupFormationOutcome::Recovered);
        let rendered = handle.render();
        assert!(
            rendered.contains("rr_formation_cas_total{result=\"conflict\"} 1"),
            "the lost activation race must count one conflict: {rendered}"
        );
        assert!(
            !rendered.contains("rr_formation_cas_total{result=\"applied\"}"),
            "no CAS may be counted as applied on the losing node: {rendered}"
        );
        assert!(
            rendered.contains("rr_formation_state{group_id=\"1\"} 1"),
            "gauge must reflect the peer's Active transition within this reconcile: {rendered}"
        );
    }
}

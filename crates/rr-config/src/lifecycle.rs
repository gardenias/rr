use std::sync::Arc;

use async_trait::async_trait;
use rr_core::{CasOutcome, ClusterLifecycle, LifecycleError, LifecycleStore, VersionedLifecycle};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Default)]
pub struct MemLifecycleStore {
    record: Arc<Mutex<Option<VersionedLifecycle>>>,
}

impl MemLifecycleStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LifecycleStore for MemLifecycleStore {
    async fn load_linearizable(&self) -> Result<Option<VersionedLifecycle>, LifecycleError> {
        Ok(self.record.lock().await.clone())
    }

    async fn create_if_absent(
        &self,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError> {
        let mut current = self.record.lock().await;
        if current.is_some() {
            return Ok(CasOutcome::Conflict);
        }
        record.validate()?;
        *current = Some(VersionedLifecycle {
            revision: 1,
            record,
        });
        Ok(CasOutcome::Applied)
    }

    async fn compare_and_swap(
        &self,
        expected_revision: u64,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError> {
        let mut current = self.record.lock().await;
        let Some(existing) = current.as_ref() else {
            return Ok(CasOutcome::Conflict);
        };
        if existing.revision != expected_revision {
            return Ok(CasOutcome::Conflict);
        }
        existing.record.validate_transition(&record)?;
        let revision = existing.revision.checked_add(1).ok_or_else(|| {
            LifecycleError::CorruptData("memory lifecycle revision overflow".to_string())
        })?;
        *current = Some(VersionedLifecycle { revision, record });
        Ok(CasOutcome::Applied)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rr_core::{
        CasOutcome, ClusterLifecycle, ClusterOrigin, FormationState, GroupFormation,
        LifecycleError, LifecycleStore,
    };

    use super::MemLifecycleStore;

    fn forming() -> ClusterLifecycle {
        ClusterLifecycle {
            version: 1,
            cluster_uuid: "12345678-1234-1234-1234-123456789abc".parse().unwrap(),
            topology_revision: 1,
            seed_hash: [7; 32],
            origin: ClusterOrigin::Genesis,
            groups: BTreeMap::from([(
                1,
                GroupFormation {
                    bootstrap_epoch: 1,
                    state: FormationState::Forming,
                },
            )]),
        }
    }

    fn active(record: &ClusterLifecycle) -> ClusterLifecycle {
        let mut active = record.clone();
        active.groups.get_mut(&1).unwrap().state = FormationState::Active;
        active
    }

    #[tokio::test]
    async fn lifecycle_clones_have_exactly_one_create_winner() {
        let store = MemLifecycleStore::new();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store.create_if_absent(forming()).await.unwrap()
            }));
        }

        let mut outcomes = Vec::new();
        for task in tasks {
            outcomes.push(task.await.unwrap());
        }
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == CasOutcome::Applied)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == CasOutcome::Conflict)
                .count(),
            7
        );
        assert_eq!(
            store.load_linearizable().await.unwrap().unwrap().revision,
            1
        );
    }

    #[tokio::test]
    async fn lifecycle_stale_cas_cannot_overwrite_active() {
        let store = MemLifecycleStore::new();
        let initial = forming();
        assert_eq!(
            store.create_if_absent(initial.clone()).await.unwrap(),
            CasOutcome::Applied
        );
        let stale = store.load_linearizable().await.unwrap().unwrap();

        assert_eq!(
            store
                .compare_and_swap(stale.revision, active(&initial))
                .await
                .unwrap(),
            CasOutcome::Applied
        );
        assert_eq!(
            store
                .compare_and_swap(stale.revision, initial)
                .await
                .unwrap(),
            CasOutcome::Conflict
        );

        let current = store.load_linearizable().await.unwrap().unwrap();
        assert!(current.revision > stale.revision);
        assert_eq!(current.record.groups[&1].state, FormationState::Active);
    }

    #[tokio::test]
    async fn lifecycle_conflict_then_reread_allows_fresh_cas() {
        let store = MemLifecycleStore::new();
        let initial = forming();
        store.create_if_absent(initial.clone()).await.unwrap();
        let stale = store.load_linearizable().await.unwrap().unwrap();

        let mut first = initial.clone();
        first.topology_revision = 2;
        assert_eq!(
            store.compare_and_swap(stale.revision, first).await.unwrap(),
            CasOutcome::Applied
        );
        assert_eq!(
            store
                .compare_and_swap(stale.revision, active(&initial))
                .await
                .unwrap(),
            CasOutcome::Conflict
        );

        let fresh = store.load_linearizable().await.unwrap().unwrap();
        assert!(fresh.revision > stale.revision);
        assert_eq!(
            store
                .compare_and_swap(fresh.revision, active(&fresh.record))
                .await
                .unwrap(),
            CasOutcome::Applied
        );
    }

    #[tokio::test]
    async fn lifecycle_active_record_persists_across_clones() {
        let store = MemLifecycleStore::new();
        let initial = forming();
        let active = active(&initial);
        store.create_if_absent(active.clone()).await.unwrap();

        let loaded = store.clone().load_linearizable().await.unwrap().unwrap();
        assert_eq!(loaded.record, active);
        assert_eq!(loaded.record.groups[&1].state, FormationState::Active);
    }

    #[tokio::test]
    async fn lifecycle_invalid_records_and_transitions_are_rejected() {
        let store = MemLifecycleStore::new();
        let mut unsupported = forming();
        unsupported.version = 2;
        assert!(matches!(
            store.create_if_absent(unsupported).await,
            Err(LifecycleError::UnsupportedSchema { .. })
        ));

        let initial = forming();
        store.create_if_absent(initial.clone()).await.unwrap();
        let current = store.load_linearizable().await.unwrap().unwrap();
        let mut added_group = initial.clone();
        added_group.groups.insert(
            2,
            GroupFormation {
                bootstrap_epoch: 1,
                state: FormationState::Forming,
            },
        );
        assert!(matches!(
            store.compare_and_swap(current.revision, added_group).await,
            Err(LifecycleError::InvalidTransition(_))
        ));

        let active = active(&initial);
        store
            .compare_and_swap(current.revision, active)
            .await
            .unwrap();
        let current = store.load_linearizable().await.unwrap().unwrap();
        assert!(matches!(
            store.compare_and_swap(current.revision, initial).await,
            Err(LifecycleError::InvalidTransition(_))
        ));
    }

    #[tokio::test]
    async fn lifecycle_independent_mem_instances_do_not_coordinate() {
        let first = MemLifecycleStore::new();
        let second = MemLifecycleStore::new();

        assert_eq!(
            first.create_if_absent(forming()).await.unwrap(),
            CasOutcome::Applied
        );
        assert_eq!(
            second.create_if_absent(forming()).await.unwrap(),
            CasOutcome::Applied
        );
        assert!(second.load_linearizable().await.unwrap().is_some());
    }
}

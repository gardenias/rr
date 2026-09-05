//! Property-based tests for `ShardRouter` under random routing-table
//! transition sequences (reliability program R-6; INV-6).
//!
//! Semantic anchors: T32/T33 in `tests/integration/src/lib.rs`
//! (`test_routing_table_update_routes_to_new_shard` /
//! `test_routing_no_messages_to_old_shard_after_update`) — re-pointing a
//! partition at a new group is just `add_partition` with the same partition
//! id and a different group id, and the update takes effect at once.
//!
//! Property under test: after EVERY op in a random
//! add/remove/reassign/membership sequence, `route()` for any partition
//! returns exactly the one group the model says is current, or a typed
//! `RouterError::PartitionNotFound` — never a panic, never a stale mapping,
//! never two groups for the same partition (the routing table is the single
//! source of truth; the router keeps no cached view).

use std::collections::HashMap;
use std::sync::Arc;

use proptest::prelude::*;
use rr_config::mem::MemConfigProvider;
use rr_core::error::{RouterError, RrError};
use rr_core::types::{GroupId, PartitionConfig, PartitionId};
use rr_router::ShardRouter;

/// Small universes so adds, removes and reassigns overlap heavily.
const PARTITION_UNIVERSE: u64 = 6;
const GROUP_UNIVERSE: u64 = 4;

#[derive(Debug, Clone)]
enum RouterOp {
    /// Register a (possibly new) partition → group mapping.
    AddPartition {
        partition: PartitionId,
        group: GroupId,
    },
    RemovePartition {
        partition: PartitionId,
    },
    /// Re-point an existing partition at a different group — the T32/T33
    /// shape; at the provider level this is `add_partition` overwriting the
    /// existing entry.
    Reassign {
        partition: PartitionId,
        group: GroupId,
    },
    /// Membership churn on a group; must not affect partition → group routing.
    UpdateMembership {
        group: GroupId,
        replicas: Vec<u64>,
    },
}

fn router_op_strategy() -> impl Strategy<Value = RouterOp> {
    let partition = 0..PARTITION_UNIVERSE;
    let group = 0..GROUP_UNIVERSE;
    prop_oneof![
        3 => (partition, group).prop_map(|(partition, group)| RouterOp::AddPartition { partition, group }),
        2 => (0..PARTITION_UNIVERSE).prop_map(|partition| RouterOp::RemovePartition { partition }),
        3 => (0..PARTITION_UNIVERSE, 0..GROUP_UNIVERSE)
            .prop_map(|(partition, group)| RouterOp::Reassign { partition, group }),
        1 => (0..GROUP_UNIVERSE, prop::collection::vec(1u64..=3, 0..=3))
            .prop_map(|(group, replicas)| RouterOp::UpdateMembership { group, replicas }),
    ]
}

fn expect_routed(
    res: Result<GroupId, RrError>,
    want: GroupId,
    ctx: &str,
) -> Result<(), TestCaseError> {
    match res {
        Ok(got) => {
            prop_assert_eq!(got, want, "{}: routed to the wrong group", ctx);
            Ok(())
        }
        Err(e) => Err(TestCaseError::fail(format!(
            "{ctx}: model says route to {want}, got Err({e})"
        ))),
    }
}

fn expect_not_found(
    res: Result<GroupId, RrError>,
    partition: PartitionId,
    ctx: &str,
) -> Result<(), TestCaseError> {
    match res {
        Err(RrError::Router(RouterError::PartitionNotFound { partition_id })) => {
            prop_assert_eq!(
                partition_id,
                partition,
                "{}: PartitionNotFound must name the queried partition",
                ctx
            );
            Ok(())
        }
        other => Err(TestCaseError::fail(format!(
            "{ctx}: partition is absent from the model, so route() must be a typed \
             PartitionNotFound — got {other:?}"
        ))),
    }
}

proptest! {
    // Bounded case count: this runs in the default CI test job (R-6 budget).
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// INV-6: after a routing-table update the old shard receives no new writes; the routing table is the single source of truth.
    #[test]
    fn proptest_router_transitions_route_consistency(
        ops in prop::collection::vec(router_op_strategy(), 1..48)
    ) {
        // House pattern for async proptests (see rr-storage): drive the async
        // provider/router from a per-case runtime inside the sync proptest fn.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let provider = MemConfigProvider::new();
            let router = ShardRouter::new(Arc::new(provider.clone()));
            // Independent model of "current partition → group mapping".
            let mut model: HashMap<PartitionId, GroupId> = HashMap::new();

            for (step, op) in ops.iter().enumerate() {
                match op {
                    RouterOp::AddPartition { partition, group }
                    | RouterOp::Reassign { partition, group } => {
                        provider
                            .add_partition(
                                *partition,
                                PartitionConfig::SingleId(*partition),
                                *group,
                                vec![1],
                            )
                            .await;
                        model.insert(*partition, *group);
                    }
                    RouterOp::RemovePartition { partition } => {
                        provider.remove_partition(*partition).await;
                        model.remove(partition);
                    }
                    RouterOp::UpdateMembership { group, replicas } => {
                        provider.update_group_membership(*group, replicas.clone()).await;
                    }
                }

                // After every op, probe the whole partition universe. Two
                // consecutive reads must agree (no cached/flapping view).
                for p in 0..PARTITION_UNIVERSE {
                    let ctx = format!("step {step} op {op:?} probe partition {p}");
                    let first = router.route(p).await;
                    let second = router.route(p).await;
                    match model.get(&p) {
                        Some(&g) => {
                            expect_routed(first, g, &ctx)?;
                            expect_routed(second, g, &ctx)?;
                        }
                        None => {
                            expect_not_found(first, p, &ctx)?;
                            expect_not_found(second, p, &ctx)?;
                        }
                    }
                }
            }

            // Final whole-table check: the router's view of the table equals
            // the model — every partition has exactly one group, no more.
            let table = router.routing_table().await.unwrap();
            let table_map: HashMap<PartitionId, GroupId> = table
                .partitions
                .iter()
                .map(|(p, (_, g))| (*p, *g))
                .collect();
            prop_assert_eq!(
                table_map,
                model,
                "routing table diverged from the op-sequence model"
            );
            // Every routed partition points at a group the table knows about
            // (MemConfigProvider creates the group entry on add/reassign).
            for (p, (_, g)) in &table.partitions {
                prop_assert!(
                    table.groups.contains_key(g),
                    "partition {} routes to group {} which is missing from table.groups",
                    p,
                    g
                );
            }
            Ok(())
        })?;
    }
}

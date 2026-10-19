use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;

use crate::Membership;
use crate::MembershipState;
use crate::RaftState;
use crate::Vote;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::raft_state::IOId;
use crate::raft_state::io_state::log_io_id::LogIOId;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::StoredMembershipOf;
use crate::utime::Leased;
use crate::vote::raft_vote::RaftVoteExt;

fn m01() -> Membership<u64, ()> {
    Membership::new_with_defaults(vec![btreeset! {0,1}], [])
}

fn m23() -> Membership<u64, ()> {
    Membership::new_with_defaults(vec![btreeset! {2,3}], [])
}

#[allow(clippy::field_reassign_with_default)]
fn new_state() -> RaftState<UTConfig> {
    let mut state = RaftState::default();

    state.vote = Leased::new(
        UTConfig::<()>::now(),
        Duration::from_millis(500),
        Vote::new_committed(2, 1),
    );
    state.apply_progress_mut().accept(log_id(1, 1, 1));
    state.membership_state = MembershipState::new(
        Arc::new(StoredMembershipOf::<UTConfig>::new(
            Some(log_id(1, 1, 1)),
            m01(),
        )),
        Arc::new(StoredMembershipOf::<UTConfig>::new(
            Some(log_id(2, 1, 3)),
            m23(),
        )),
    );
    state
}

#[test]
fn test_update_committed_none() -> anyhow::Result<()> {
    let mut state = new_state();
    let committed_vote = state.vote_ref().to_committed();

    state.update_committed(LogIOId::new(committed_vote, None));

    assert_eq!(Some(&log_id(1, 1, 1)), state.local_committed());
    assert_eq!(
        MembershipState::new(
            Arc::new(StoredMembershipOf::<UTConfig>::new(
                Some(log_id(1, 1, 1)),
                m01()
            )),
            Arc::new(StoredMembershipOf::<UTConfig>::new(
                Some(log_id(2, 1, 3)),
                m23()
            )),
        ),
        state.membership_state
    );
    assert_eq!(
        Some(&log_id(1, 1, 1)),
        state.io_state.apply_progress.accepted()
    );
    assert_eq!(None, state.io_state.apply_progress.submitted());

    Ok(())
}

#[test]
fn test_update_committed_ge_accepted() -> anyhow::Result<()> {
    let mut state = new_state();
    let committed_vote = state.vote_ref().to_committed();
    state.log_progress_mut().accept(IOId::new_log_io(
        committed_vote.clone(),
        Some(log_id(1, 1, 2)),
    ));

    state.update_committed(LogIOId::new(committed_vote, Some(log_id(2, 1, 3))));

    // Local committed is min(accepted, cluster_committed) = min(log_id(1,1,2), log_id(2,1,3)) =
    // log_id(1,1,2)
    assert_eq!(Some(&log_id(1, 1, 2)), state.local_committed());
    assert_eq!(
        MembershipState::new(
            Arc::new(StoredMembershipOf::<UTConfig>::new(
                Some(log_id(1, 1, 1)),
                m01()
            )),
            Arc::new(StoredMembershipOf::<UTConfig>::new(
                Some(log_id(2, 1, 3)),
                m23()
            )),
        ),
        state.membership_state
    );
    assert_eq!(
        Some(&log_id(1, 1, 2)),
        state.io_state.apply_progress.accepted()
    );
    assert_eq!(None, state.io_state.apply_progress.submitted());

    Ok(())
}

#[test]
fn test_update_committed_le_accepted() -> anyhow::Result<()> {
    let mut state = new_state();
    let committed_vote = state.vote_ref().to_committed();
    state.log_progress_mut().accept(IOId::new_log_io(
        committed_vote.clone(),
        Some(log_id(3, 1, 4)),
    ));

    state.update_committed(LogIOId::new(committed_vote, Some(log_id(2, 1, 3))));

    // Local committed is min(accepted, cluster_committed) = min(log_id(3,1,4), log_id(2,1,3)) =
    // log_id(2,1,3)
    assert_eq!(Some(&log_id(2, 1, 3)), state.local_committed());
    assert_eq!(
        MembershipState::new(
            Arc::new(StoredMembershipOf::<UTConfig>::new(
                Some(log_id(2, 1, 3)),
                m23()
            )),
            Arc::new(StoredMembershipOf::<UTConfig>::new(
                Some(log_id(2, 1, 3)),
                m23()
            ))
        ),
        state.membership_state
    );
    assert_eq!(
        Some(&log_id(2, 1, 3)),
        state.io_state.apply_progress.accepted()
    );
    assert_eq!(None, state.io_state.apply_progress.submitted());

    Ok(())
}

#[test]
fn test_update_committed_local_vote_lags_cluster_vote() -> anyhow::Result<()> {
    let mut state = new_state();
    let committed_vote = state.vote_ref().to_committed();
    state.log_progress_mut().accept(IOId::new_log_io(
        committed_vote.clone(),
        Some(log_id(2, 1, 3)),
    ));

    let higher_vote = Vote::new_committed(3, 2).to_committed();
    let cluster_committed = LogIOId::new(higher_vote.clone(), Some(log_id(3, 2, 4)));

    state.update_committed(cluster_committed.clone());

    // Local committed must not advance until an IO accepted under the new leader vote arrives.
    assert_eq!(Some(&log_id(1, 1, 1)), state.local_committed());
    assert_eq!(
        Some(cluster_committed),
        state.io_state.cluster_committed.value().cloned()
    );
    assert_eq!(
        Some(&log_id(1, 1, 1)),
        state.io_state.apply_progress.accepted()
    );
    assert_eq!(None, state.io_state.apply_progress.submitted());

    Ok(())
}

#[test]
fn test_update_committed_advances_after_local_vote_catches_up() -> anyhow::Result<()> {
    let mut state = new_state();
    let committed_vote = state.vote_ref().to_committed();
    state.log_progress_mut().accept(IOId::new_log_io(
        committed_vote.clone(),
        Some(log_id(2, 1, 3)),
    ));

    let higher_vote = Vote::new_committed(3, 2).to_committed();
    let cluster_committed = LogIOId::new(higher_vote.clone(), Some(log_id(3, 2, 4)));

    // Commit notification arrives before logs from the new leader; no local advancement yet.
    state.update_committed(cluster_committed.clone());
    assert_eq!(Some(&log_id(1, 1, 1)), state.local_committed());

    // Later the follower accepts the log written by the new leader.
    state
        .log_progress_mut()
        .accept(IOId::new_log_io(higher_vote.clone(), Some(log_id(3, 2, 4))));

    // Re-evaluating committed now that votes match should advance local committed.
    state.update_committed(cluster_committed.clone());

    assert_eq!(Some(&log_id(3, 2, 4)), state.local_committed());
    assert_eq!(
        Some(&log_id(3, 2, 4)),
        state.io_state.apply_progress.accepted()
    );
    assert_eq!(None, state.io_state.apply_progress.submitted());

    Ok(())
}

#[test]
fn test_committed_accessors() -> anyhow::Result<()> {
    // Fresh state: neither commit is known yet.
    {
        let state = RaftState::<UTConfig>::default();
        assert_eq!(None, state.local_committed());
        assert_eq!(None, state.cluster_committed());
    }

    // Restored node: `local_committed` is restored from storage (the apply ceiling), but
    // `cluster_committed` is never restored — it stays None until a quorum re-grants a commit. This
    // guards the invariant that a non-null `cluster_committed` is always a genuine, freshly granted
    // cluster commit, never a value read back from `RaftStorage`.
    {
        let state = new_state();
        assert_eq!(Some(&log_id(1, 1, 1)), state.local_committed());
        assert_eq!(None, state.cluster_committed());
    }

    // Caught-up node: the accepted log id reaches the quorum-granted commit, so the two agree.
    {
        let mut state = new_state();
        let committed_vote = state.vote_ref().to_committed();
        state.log_progress_mut().accept(IOId::new_log_io(
            committed_vote.clone(),
            Some(log_id(2, 1, 3)),
        ));

        state.update_committed(LogIOId::new(committed_vote, Some(log_id(2, 1, 3))));

        assert_eq!(Some(&log_id(2, 1, 3)), state.cluster_committed());
        assert_eq!(Some(&log_id(2, 1, 3)), state.local_committed());
    }

    // Lagging follower: a commit granted under a newer leader vote arrives before its logs, so
    // cluster-committed leads local-committed.
    {
        let mut state = new_state();
        let committed_vote = state.vote_ref().to_committed();
        state
            .log_progress_mut()
            .accept(IOId::new_log_io(committed_vote, Some(log_id(2, 1, 3))));

        let higher_vote = Vote::new_committed(3, 2).to_committed();
        state.update_committed(LogIOId::new(higher_vote, Some(log_id(3, 2, 4))));

        assert_eq!(Some(&log_id(3, 2, 4)), state.cluster_committed());
        assert_eq!(Some(&log_id(1, 1, 1)), state.local_committed());

        // The deprecated `committed()` mirrors `local_committed()`, not `cluster_committed()`.
        #[allow(deprecated)]
        {
            assert_eq!(state.local_committed(), state.committed());
        }
    }

    Ok(())
}

/// rr patch #5 (2026-10-19, committed index regression guard): a commit
/// notification from a higher-vote leader at a LOWER index must not move the
/// local committed cursor backwards. This is the anatomy of soak run
/// 38045591646, where an illegitimate short-log leader pushed followers'
/// committed from index 1434247 down to 392176 — `LogIOId`/`LogId`
/// lexicographic (vote, index) ordering alone would adopt the regression.
#[test]
fn test_update_committed_rejects_index_regression_from_higher_vote() -> anyhow::Result<()> {
    let mut state = new_state();
    let committed_vote = state.vote_ref().to_committed();

    // Legitimately advance local committed to index 100 under vote (2,1).
    state.log_progress_mut().accept(IOId::new_log_io(
        committed_vote.clone(),
        Some(log_id(2, 1, 100)),
    ));
    state.update_committed(LogIOId::new(committed_vote, Some(log_id(2, 1, 100))));
    assert_eq!(Some(&log_id(2, 1, 100)), state.local_committed());

    // The follower has accepted the short log of a higher-vote leader (3,2)
    // (its in-memory log ids were already truncated down to the conflicting
    // prefix), and the leader reports leader_commit at index 50.
    let short_leader_vote = Vote::new_committed(3, 2).to_committed();
    state.log_progress_mut().accept(IOId::new_log_io(
        short_leader_vote.clone(),
        Some(log_id(3, 2, 50)),
    ));
    state.update_committed(LogIOId::new(short_leader_vote, Some(log_id(3, 2, 50))));

    // The committed cursor stays at index 100: the regression is rejected.
    assert_eq!(Some(&log_id(2, 1, 100)), state.local_committed());
    assert_eq!(
        Some(&log_id(2, 1, 100)),
        state.io_state.apply_progress.accepted()
    );

    Ok(())
}

/// rr patch #5 续：same-vote updates keep the original comparison outcome —
/// a higher index still advances and a lower index is still not adopted.
#[test]
fn test_update_committed_same_vote_index_progression() -> anyhow::Result<()> {
    let mut state = new_state();
    let committed_vote = state.vote_ref().to_committed();

    state.log_progress_mut().accept(IOId::new_log_io(
        committed_vote.clone(),
        Some(log_id(2, 1, 100)),
    ));
    state.update_committed(LogIOId::new(
        committed_vote.clone(),
        Some(log_id(2, 1, 100)),
    ));
    assert_eq!(Some(&log_id(2, 1, 100)), state.local_committed());

    // Same vote, higher index: advances.
    state.log_progress_mut().accept(IOId::new_log_io(
        committed_vote.clone(),
        Some(log_id(2, 1, 150)),
    ));
    state.update_committed(LogIOId::new(committed_vote, Some(log_id(2, 1, 150))));
    assert_eq!(Some(&log_id(2, 1, 150)), state.local_committed());

    // Same vote, lower index: not adopted (outcome unchanged from upstream).
    state.update_local_committed(&Some(log_id(2, 1, 120)));
    assert_eq!(Some(&log_id(2, 1, 150)), state.local_committed());

    Ok(())
}

/// rr patch #5 续：a legitimate leader change still advances committed — by
/// leader completeness the new leader holds every committed entry, so its
/// commit carries a higher index under a higher vote.
#[test]
fn test_update_committed_higher_vote_higher_index_advances() -> anyhow::Result<()> {
    let mut state = new_state();
    let committed_vote = state.vote_ref().to_committed();

    state.log_progress_mut().accept(IOId::new_log_io(
        committed_vote.clone(),
        Some(log_id(2, 1, 100)),
    ));
    state.update_committed(LogIOId::new(committed_vote, Some(log_id(2, 1, 100))));
    assert_eq!(Some(&log_id(2, 1, 100)), state.local_committed());

    let new_leader_vote = Vote::new_committed(3, 2).to_committed();
    state.log_progress_mut().accept(IOId::new_log_io(
        new_leader_vote.clone(),
        Some(log_id(3, 2, 150)),
    ));
    state.update_committed(LogIOId::new(new_leader_vote, Some(log_id(3, 2, 150))));

    assert_eq!(Some(&log_id(3, 2, 150)), state.local_committed());
    assert_eq!(
        Some(&log_id(3, 2, 150)),
        state.io_state.apply_progress.accepted()
    );

    Ok(())
}

/// rr patch #5 续：guard semantics at the `update_local_committed` funnel —
/// the leader-side commit path (`try_commit_quorum_accepted`) also flows
/// through here. Only the index matters: a lower index is rejected whatever
/// the vote; an equal index under a different vote is not a regression and
/// keeps the upstream lexicographic behavior.
#[test]
fn test_update_local_committed_index_never_regresses() -> anyhow::Result<()> {
    let mut state = new_state();

    state.update_local_committed(&Some(log_id(2, 1, 100)));
    assert_eq!(Some(&log_id(2, 1, 100)), state.local_committed());

    // Higher vote, lower index: rejected.
    state.update_local_committed(&Some(log_id(3, 2, 50)));
    assert_eq!(Some(&log_id(2, 1, 100)), state.local_committed());

    // Lower vote, lower index: rejected as well (upstream ordering would
    // already have ignored it).
    state.update_local_committed(&Some(log_id(1, 1, 80)));
    assert_eq!(Some(&log_id(2, 1, 100)), state.local_committed());

    // Same index under a higher vote: not an index regression; adopted.
    state.update_local_committed(&Some(log_id(3, 2, 100)));
    assert_eq!(Some(&log_id(3, 2, 100)), state.local_committed());

    // Higher index: adopted.
    state.update_local_committed(&Some(log_id(3, 2, 101)));
    assert_eq!(Some(&log_id(3, 2, 101)), state.local_committed());

    // None is a no-op.
    state.update_local_committed(&None);
    assert_eq!(Some(&log_id(3, 2, 101)), state.local_committed());

    Ok(())
}

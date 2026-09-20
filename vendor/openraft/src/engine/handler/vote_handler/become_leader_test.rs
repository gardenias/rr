use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::Membership;
use crate::Vote;
use crate::batch::Batch;
use crate::core::ServerState;
use crate::engine::Command;
use crate::engine::Condition;
use crate::engine::Engine;
use crate::engine::TargetProgress;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::entry::RaftEntry;
use crate::progress::entry::ProgressEntry;
use crate::progress::inflight_id::InflightId;
use crate::progress::stream_id::StreamId;
use crate::raft_state::IOId;
use crate::replication::payload::Payload;
use crate::replication::replicate::Replicate;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::EntryOf;
use crate::type_config::alias::StoredMembershipOf;
use crate::utime::Leased;
use crate::vote::raft_vote::RaftVoteExt;

fn m01() -> Membership<u64, ()> {
    Membership::<u64, ()>::new_with_defaults(vec![btreeset! {0,1}], [])
}

fn eng() -> Engine<UTConfig> {
    let mut eng = Engine::testing_default(0);
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = 1;
    eng.state.vote = Leased::new(
        UTConfig::<()>::now(),
        Duration::from_millis(500),
        Vote::new_committed(2, 1),
    );
    eng.state.server_state = ServerState::Candidate;
    eng.state.membership_state.set_effective(Arc::new(StoredMembershipOf::<UTConfig>::new(
        Some(log_id(1, 1, 1)),
        m01(),
    )));

    eng.output.take_commands();
    eng
}

#[test]
fn test_become_leader() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.vote_handler().become_leader();

    let leader = eng.leader.as_ref().unwrap();
    assert_eq!(leader.noop_log_id, log_id(2, 1, 0));
    assert_eq!(leader.last_log_id(), Some(&log_id(2, 1, 0)));
    assert_eq!(*leader.committed_vote_ref(), Vote::new(2, 1).to_committed());

    assert_eq!(ServerState::Leader, eng.state.server_state);

    assert_eq!(eng.output.take_commands(), vec![
        Command::UpdateIOProgress {
            when: None,
            io_id: IOId::new_log_io(Vote::new(2, 1).to_committed(), None)
        },
        Command::RebuildReplicationStreams {
            leader_vote: Vote::new(2, 1).to_committed(),
            targets: vec![TargetProgress {
                target: 0,
                target_node: (),
                progress: ProgressEntry::empty(0, StreamId::new(1), 0),
            }],
            close_old_streams: true,
        },
        Command::AppendEntries {
            committed_vote: Vote::new(2, 1).to_committed(),
            entries: Batch::of([EntryOf::<UTConfig>::new_blank(log_id(2, 1, 0))])
        },
        // Pipeline mode: ProgressEntry::empty(0) has matching.next_index()=0 == searching_end=0
        Command::Replicate {
            target: 0,
            req: Replicate {
                inflight_id: InflightId::new(1),
                payload: Payload::LogsSince { prev: None },
            }
        }
    ]);

    Ok(())
}

/// rr patch #3 (2026-09-21): when a log IO was accepted before the election
/// (i.e. it may still be unflushed on stores with deferred append
/// completion), the become-leader `UpdateIOProgress` must be gated on that
/// IO's real flush — otherwise `handle_local_io` would mark the unflushed
/// tail as flushed and the new leader could self-match entries that are not
/// durable anywhere on this node.
#[test]
fn test_become_leader_gates_io_progress_on_prior_accepted_io() -> anyhow::Result<()> {
    let mut eng = eng();

    // A previously accepted log IO, standing in for an in-flight append.
    let prev = IOId::new_log_io(Vote::new(1, 1).to_committed(), Some(log_id(1, 1, 3)));
    eng.state.accept_log_io(prev.clone());

    eng.vote_handler().become_leader();

    let cmds = eng.output.take_commands();
    let Some(Command::UpdateIOProgress { when, io_id }) = cmds.first() else {
        panic!("first command must be UpdateIOProgress, got: {cmds:?}");
    };
    assert_eq!(
        *when,
        Some(Condition::IOFlushed { io_id: prev }),
        "the update must wait for the pre-election IO to flush"
    );
    assert_eq!(
        *io_id,
        IOId::new_log_io(Vote::new(2, 1).to_committed(), None),
        "the io being marked is still the leader's current tip"
    );

    Ok(())
}

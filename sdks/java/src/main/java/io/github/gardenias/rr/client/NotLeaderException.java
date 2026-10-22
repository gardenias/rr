package io.github.gardenias.rr.client;

/**
 * Thrown when a write exhausted its retry budget without reaching the group
 * leader. Carries the last redirect hint received; both fields are null when
 * every contacted node was mid-election and never knew a leader (mirrors the
 * Rust {@code RaftError::NotLeader { leader: None, leader_addr: None }}).
 */
public final class NotLeaderException extends RrClientException {
    private final Long leaderNodeId;
    private final String leaderAddr;

    public NotLeaderException(Long leaderNodeId, String leaderAddr) {
        super("not leader, current leader: id=" + leaderNodeId + ", addr=" + leaderAddr);
        this.leaderNodeId = leaderNodeId;
        this.leaderAddr = leaderAddr;
    }

    /** Leader node id of the last redirect hint; null when no node knew a leader. */
    public Long leaderNodeId() {
        return leaderNodeId;
    }

    /** Leader address of the last redirect hint; null when unknown. */
    public String leaderAddr() {
        return leaderAddr;
    }
}

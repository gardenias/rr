package io.github.gardenias.rr.client;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class LeaderCache {
    private final ConcurrentMap<Long, LeaderInfo> byGroup = new ConcurrentHashMap<>();

    void update(long groupId, long nodeId, String addr) {
        byGroup.put(groupId, new LeaderInfo(nodeId, addr));
    }

    Optional<LeaderInfo> get(long groupId) {
        return Optional.ofNullable(byGroup.get(groupId));
    }

    void remove(long groupId) {
        byGroup.remove(groupId);
    }

    record LeaderInfo(long nodeId, String addr) {}
}

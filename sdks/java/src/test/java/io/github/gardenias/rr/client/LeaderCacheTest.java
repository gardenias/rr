package io.github.gardenias.rr.client;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LeaderCacheTest {

    @Test
    void updateStores() {
        LeaderCache cache = new LeaderCache();
        cache.update(1L, 42L, "10.0.0.1:51001");

        assertThat(cache.get(1L)).isPresent();
        assertThat(cache.get(1L).orElseThrow().nodeId()).isEqualTo(42L);
        assertThat(cache.get(1L).orElseThrow().addr()).isEqualTo("10.0.0.1:51001");
    }

    @Test
    void removeEvicts() {
        LeaderCache cache = new LeaderCache();
        cache.update(1L, 42L, "10.0.0.1:51001");

        cache.remove(1L);
        assertThat(cache.get(1L)).isEmpty();
    }

    @Test
    void multipleGroupsIsolated() {
        LeaderCache cache = new LeaderCache();
        cache.update(1L, 11L, "10.0.0.11:51001");
        cache.update(2L, 22L, "10.0.0.22:51001");

        assertThat(cache.get(1L).orElseThrow().nodeId()).isEqualTo(11L);
        assertThat(cache.get(2L).orElseThrow().nodeId()).isEqualTo(22L);

        cache.remove(1L);
        assertThat(cache.get(1L)).isEmpty();
        assertThat(cache.get(2L)).isPresent();
    }
}

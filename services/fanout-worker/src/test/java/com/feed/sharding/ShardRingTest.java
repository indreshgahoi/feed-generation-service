package com.feed.sharding;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ShardRingTest {

    private static ShardTopology topologyWith(int numShards, int vnodes) {
        ShardTopology topology = new ShardTopology();
        topology.virtualNodesPerShard = vnodes;
        topology.shards = new java.util.ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            ShardConfig shard = new ShardConfig();
            shard.id = i;
            topology.shards.add(shard);
        }
        return topology;
    }

    @Test
    void shardForNewEntity_isDeterministic() {
        ShardRing ring = new ShardRing(topologyWith(4, 150));
        for (int i = 0; i < 1000; i++) {
            String key = "user-" + i;
            int first = ring.shardForNewEntity(key);
            int second = ring.shardForNewEntity(key);
            assertEquals(first, second, "shardForNewEntity(" + key + ") not deterministic");
            assertTrue(first >= 0 && first < 4, "shard out of range: " + first);
        }
    }

    @Test
    void shardForNewEntity_roughlyEvenDistribution() {
        ShardRing ring = new ShardRing(topologyWith(4, 150));
        Map<Integer, Integer> counts = new HashMap<>();
        int n = 50000;
        for (int i = 0; i < n; i++) {
            int shard = ring.shardForNewEntity("placement-key-" + i);
            counts.merge(shard, 1, Integer::sum);
        }
        double expected = n / 4.0;
        for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
            double deviation = Math.abs(entry.getValue() - expected) / expected;
            assertTrue(deviation < 0.15,
                "shard " + entry.getKey() + " got " + entry.getValue() +
                    " placements, deviation " + (deviation * 100) + "% exceeds 15% tolerance");
        }
    }

    @Test
    void numShards_matchesTopology() {
        ShardRing ring = new ShardRing(topologyWith(4, 150));
        assertEquals(4, ring.numShards());
    }
}

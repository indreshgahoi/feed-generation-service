package com.feed.sharding;

import java.nio.charset.StandardCharsets;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * Consistent-hash ring used for exactly one decision: which shard a
 * brand-new entity gets created on. This MUST produce bit-for-bit
 * identical results to pkg/sharding's Go implementation for the same
 * config -- see /doc/sharding.md and scripts/verify_shard_parity.sh.
 *
 * <p>Java's {@link CRC32} computes the same IEEE/CRC-32 polynomial as
 * Go's {@code hash/crc32.ChecksumIEEE} (the zlib/gzip standard), so no
 * external dependency is needed in either language for the two
 * implementations to agree.
 */
public class ShardRing {
    private final SortedMap<Long, Integer> ring = new TreeMap<>();
    private final int numShards;

    public ShardRing(ShardTopology topology) {
        this.numShards = topology.shards.size();
        for (ShardConfig shard : topology.shards) {
            for (int v = 0; v < topology.virtualNodesPerShard; v++) {
                // Key order (vnode index first, then shard id) matters --
                // see the comment in pkg/sharding/ring.go for why the
                // reverse order measurably clusters under CRC32.
                String key = "vnode-" + v + "-shard-" + shard.id;
                ring.put(crc32(key), shard.id);
            }
        }
    }

    private static long crc32(String s) {
        CRC32 crc = new CRC32();
        crc.update(s.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }

    /**
     * Returns the shard a new entity should be created on, given a
     * caller-chosen placement key (e.g. a signup UUID or username).
     */
    public int shardForNewEntity(String placementKey) {
        long h = crc32(placementKey);
        SortedMap<Long, Integer> tail = ring.tailMap(h);
        Long ringKey = tail.isEmpty() ? ring.firstKey() : tail.firstKey();
        return ring.get(ringKey);
    }

    public int numShards() {
        return numShards;
    }
}

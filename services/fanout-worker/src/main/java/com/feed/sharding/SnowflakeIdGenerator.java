package com.feed.sharding;

/**
 * Mirrors pkg/sharding's Go IDGenerator bit-for-bit: 41 bits timestamp |
 * 8 bits shard ID | 14 bits sequence, same epoch. Any service that mints
 * IDs (in any language) must use this exact layout, or ExtractShardId
 * would recover the wrong shard for IDs minted by whichever service
 * drifted. See /doc/DESIGN.md.
 */
public class SnowflakeIdGenerator {
    private static final long EPOCH_MILLIS = 1704067200000L; // 2024-01-01T00:00:00Z
    private static final int SEQUENCE_BITS = 14;
    private static final int SHARD_ID_BITS = 8;
    private static final int SHARD_ID_SHIFT = SEQUENCE_BITS;
    private static final int TIMESTAMP_SHIFT = SEQUENCE_BITS + SHARD_ID_BITS;
    private static final int MAX_SHARD_ID = (1 << SHARD_ID_BITS) - 1;
    private static final long MAX_SEQUENCE = (1L << SEQUENCE_BITS) - 1;

    private final int shardId;
    private long sequence = 0;
    private long lastTimestamp = -1;

    public SnowflakeIdGenerator(int shardId) {
        if (shardId < 0 || shardId > MAX_SHARD_ID) {
            throw new IllegalArgumentException("sharding: shardId out of range for an 8-bit ID field: " + shardId);
        }
        this.shardId = shardId;
    }

    public synchronized long next() {
        long ts = System.currentTimeMillis() - EPOCH_MILLIS;
        if (ts == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                while (ts <= lastTimestamp) {
                    ts = System.currentTimeMillis() - EPOCH_MILLIS;
                }
            }
        } else {
            sequence = 0;
        }
        lastTimestamp = ts;
        return (ts << TIMESTAMP_SHIFT) | ((long) shardId << SHARD_ID_SHIFT) | sequence;
    }

    /** Recovers the shard that minted id -- a bit-shift, never a lookup. */
    public static int extractShardId(long id) {
        return (int) ((id >> SHARD_ID_SHIFT) & MAX_SHARD_ID);
    }
}

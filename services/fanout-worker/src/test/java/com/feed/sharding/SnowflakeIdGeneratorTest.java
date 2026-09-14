package com.feed.sharding;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SnowflakeIdGeneratorTest {

    @Test
    void extractShardId_roundTrips() {
        for (int shardId = 0; shardId <= 255; shardId += 17) {
            SnowflakeIdGenerator gen = new SnowflakeIdGenerator(shardId);
            long id = gen.next();
            assertEquals(shardId, SnowflakeIdGenerator.extractShardId(id),
                "shard id did not round-trip through the generated ID");
        }
    }

    @Test
    void next_isMonotonicWithinShard() {
        SnowflakeIdGenerator gen = new SnowflakeIdGenerator(2);
        long last = -1;
        for (int i = 0; i < 10000; i++) {
            long id = gen.next();
            assertTrue(id > last, "ID generator produced non-increasing ID: " + id + " after " + last);
            last = id;
        }
    }

    @Test
    void constructor_rejectsOutOfRangeShardId() {
        assertThrows(IllegalArgumentException.class, () -> new SnowflakeIdGenerator(256));
        assertThrows(IllegalArgumentException.class, () -> new SnowflakeIdGenerator(-1));
    }
}

package com.feed.fanout.storage.redis;

import com.feed.fanout.domain.HotInboxRepository;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

/**
 * Implements domain.HotInboxRepository against Redis -- see
 * doc/caching.md. The hot tier for followers active within
 * ACTIVE_WITHIN_DAYS; the cold tier (dormant followers) is a separate
 * client (ColdTierHttpClient) since it's owned by a different service.
 */
public class RedisHotInboxRepository implements HotInboxRepository {
    private final JedisPool pool;

    public RedisHotInboxRepository(JedisPool pool) {
        this.pool = pool;
    }

    @Override
    public void pushToInbox(long followerId, long postId, long score, int maxItems) {
        String key = "feed:user:" + followerId;
        try (Jedis jedis = pool.getResource()) {
            jedis.zadd(key, score, String.valueOf(postId));
            // Keep only the newest maxItems entries per inbox.
            jedis.zremrangeByRank(key, 0, -(maxItems + 1));
        }
    }

    @Override
    public void pushToCelebrityOutbox(long authorId, long postId, long score) {
        String key = "celebrity:outbox:" + authorId;
        try (Jedis jedis = pool.getResource()) {
            jedis.zadd(key, score, String.valueOf(postId));
        }
    }
}

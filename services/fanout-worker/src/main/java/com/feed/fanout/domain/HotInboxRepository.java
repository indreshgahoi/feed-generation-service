package com.feed.fanout.domain;

/**
 * Backed by Redis -- the hot tier for users active within
 * ACTIVE_WITHIN_DAYS. See doc/caching.md.
 */
public interface HotInboxRepository {
    /** ZADD into an active follower's own inbox, then trim to maxItems. */
    void pushToInbox(long followerId, long postId, long score, int maxItems);

    /** ZADD into the celebrity's own outbox -- followers pull at read time. */
    void pushToCelebrityOutbox(long authorId, long postId, long score);
}

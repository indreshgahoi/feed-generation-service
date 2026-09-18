package com.feed.fanout.domain;

/**
 * Calls feed-aggregation-service's internal cold-tier endpoint over
 * HTTP. BadgerDB (the cold tier's storage) is a Go-only embedded
 * library, so a cross-language write has to go over the network rather
 * than a shared library call -- see doc/DESIGN.md.
 */
public interface ColdTierClient {
    /**
     * Appends a fan-out entry to a DORMANT follower's cold-tier inbox,
     * instead of the pre-tiering behavior of simply dropping that
     * fan-out write. See doc/DESIGN.md.
     */
    void appendToColdTier(long userId, long postId, long authorId);
}

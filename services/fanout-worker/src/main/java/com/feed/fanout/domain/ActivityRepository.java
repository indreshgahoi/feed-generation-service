package com.feed.fanout.domain;

import java.util.List;

/**
 * Backed by sharded Postgres. Followers can live on any of the 4 shards
 * regardless of which shard the author (or Neo4j) put them on, so
 * filtering a follower list down to "active within N days" is a
 * cross-shard scatter-gather: group by shard (bit-shift, no lookup), fan
 * out, merge. See doc/sharding.md.
 */
public interface ActivityRepository {
    /** Returns the subset of userIds active within activeWithinDays. */
    List<Long> filterActive(List<Long> userIds, int activeWithinDays);
}

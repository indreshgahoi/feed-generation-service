package com.feed.fanout.domain;

import java.util.List;

/**
 * Backed by Neo4j, not sharded Postgres -- see doc/sharding.md "The
 * social graph lives in Neo4j, not sharded Postgres". Both queries this
 * worker needs (is the author a celebrity, who follows them) are single
 * Cypher queries regardless of how many shards the author's followers'
 * profile data is spread across -- that's the whole point of not
 * hand-rolling the follow graph relationally.
 */
public interface SocialGraphRepository {
    boolean isCelebrity(long authorId);

    /** Followers of authorId -- the hot-path query, run on every post. */
    List<Long> getFollowers(long authorId);
}

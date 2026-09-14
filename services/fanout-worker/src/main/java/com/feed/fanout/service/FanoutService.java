package com.feed.fanout.service;

import com.feed.fanout.domain.ActivityRepository;
import com.feed.fanout.domain.ColdTierClient;
import com.feed.fanout.domain.HotInboxRepository;
import com.feed.fanout.domain.PostCreatedEvent;
import com.feed.fanout.domain.SocialGraphRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * The hybrid fan-out decision from the design doc (§3.4), plus the
 * hot/cold tiering from doc/caching.md -- this is the one place both
 * policies actually live, independent of Postgres/Neo4j/Redis/HTTP
 * specifics, which is what makes it unit-testable with mocked
 * repositories (see FanoutServiceTest).
 */
public class FanoutService {
    private static final Logger log = LoggerFactory.getLogger(FanoutService.class);

    private final SocialGraphRepository socialGraph;
    private final ActivityRepository activity;
    private final HotInboxRepository hotInbox;
    private final ColdTierClient coldTier;
    private final int feedInboxMaxItems;
    private final int activeWithinDays;

    public FanoutService(
            SocialGraphRepository socialGraph,
            ActivityRepository activity,
            HotInboxRepository hotInbox,
            ColdTierClient coldTier,
            int feedInboxMaxItems,
            int activeWithinDays) {
        this.socialGraph = socialGraph;
        this.activity = activity;
        this.hotInbox = hotInbox;
        this.coldTier = coldTier;
        this.feedInboxMaxItems = feedInboxMaxItems;
        this.activeWithinDays = activeWithinDays;
    }

    public void handle(PostCreatedEvent event) {
        long authorId = Long.parseLong(event.userId);
        long postId = Long.parseLong(event.postId);
        long score = OffsetDateTime.parse(event.createdAt).toInstant().toEpochMilli();

        // Celebrity status now lives on the Neo4j User node (see
        // doc/sharding.md) -- one graph query, no Postgres shard lookup
        // needed for this decision at all.
        if (socialGraph.isCelebrity(authorId)) {
            hotInbox.pushToCelebrityOutbox(authorId, postId, score);
            log.info("post {} -> celebrity outbox for author {}", postId, authorId);
            return;
        }

        // "Who follows me" is a single Neo4j query regardless of which
        // Postgres shard each follower's own profile lives on -- the
        // simplification a graph database buys over the cross-shard
        // follows_incoming design this repo tried and abandoned. See
        // doc/sharding.md.
        List<Long> followers = socialGraph.getFollowers(authorId);
        if (followers.isEmpty()) {
            log.info("post {} by {} has no followers to fan out to", postId, authorId);
            return;
        }

        List<Long> activeFollowers = activity.filterActive(followers, activeWithinDays);
        List<Long> dormantFollowers = new ArrayList<>(followers);
        dormantFollowers.removeAll(activeFollowers);

        for (Long followerId : activeFollowers) {
            hotInbox.pushToInbox(followerId, postId, score, feedInboxMaxItems);
        }
        // Dormant followers used to simply be dropped here (the
        // pre-tiering behavior). Now their fan-out entry goes to the
        // cold tier instead, so it isn't lost -- just cheaper to store,
        // and promoted back to the hot tier if/when they return. See
        // doc/caching.md.
        for (Long followerId : dormantFollowers) {
            coldTier.appendToColdTier(followerId, postId, authorId);
        }

        log.info("post {} fanned out to {} active + {} dormant (cold-tier) followers of {}",
                postId, activeFollowers.size(), dormantFollowers.size(), authorId);
    }
}

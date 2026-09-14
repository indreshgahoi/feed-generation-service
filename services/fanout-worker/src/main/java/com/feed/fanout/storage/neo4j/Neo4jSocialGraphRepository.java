package com.feed.fanout.storage.neo4j;

import com.feed.fanout.domain.SocialGraphRepository;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implements domain.SocialGraphRepository against Neo4j -- see
 * doc/sharding.md "The social graph lives in Neo4j, not sharded
 * Postgres". Both queries here replace what a relationally-sharded
 * design would need cross-shard scatter-gather for.
 */
public class Neo4jSocialGraphRepository implements SocialGraphRepository, AutoCloseable {
    private final Driver driver;

    public Neo4jSocialGraphRepository(String uri, String username, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
    }

    public void verifyConnectivity() {
        driver.verifyConnectivity();
    }

    @Override
    public void close() {
        driver.close();
    }

    @Override
    public boolean isCelebrity(long authorId) {
        try (Session session = driver.session()) {
            Record record = session.run(
                    "MATCH (u:User {userId: $userId}) RETURN u.isCelebrity AS isCelebrity",
                    Map.of("userId", authorId)
            ).single();
            return record.get("isCelebrity").asBoolean(false);
        }
    }

    @Override
    public List<Long> getFollowers(long authorId) {
        try (Session session = driver.session()) {
            List<Long> followers = new ArrayList<>();
            session.run(
                    "MATCH (follower:User)-[:FOLLOWS]->(:User {userId: $userId}) RETURN follower.userId AS followerId",
                    Map.of("userId", authorId)
            ).forEachRemaining(record -> followers.add(record.get("followerId").asLong()));
            return followers;
        }
    }
}

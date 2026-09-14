package com.feed.fanout;

public final class Config {
    public final String kafkaBrokers;
    public final String shardConfigPath;
    public final String redisHost;
    public final int redisPort;
    public final String neo4jUri;
    public final String neo4jUsername;
    public final String neo4jPassword;
    public final String feedAggregationServiceUrl;
    public final int feedInboxMaxItems;
    public final int activeWithinDays;

    private Config() {
        this.kafkaBrokers = env("KAFKA_BROKERS", "localhost:9092");
        this.shardConfigPath = env("SHARD_CONFIG_PATH", "../../config/shards.json");
        this.redisHost = env("REDIS_HOST", "localhost");
        this.redisPort = Integer.parseInt(env("REDIS_PORT", "6379"));
        this.neo4jUri = env("NEO4J_URI", "bolt://localhost:7687");
        this.neo4jUsername = env("NEO4J_USERNAME", "neo4j");
        this.neo4jPassword = env("NEO4J_PASSWORD", "feedpassword");
        this.feedAggregationServiceUrl = env("FEED_AGGREGATION_SERVICE_URL", "http://localhost:4002");
        this.feedInboxMaxItems = Integer.parseInt(env("FEED_INBOX_MAX_ITEMS", "800"));
        this.activeWithinDays = Integer.parseInt(env("ACTIVE_WITHIN_DAYS", "7"));
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }

    public static Config load() {
        return new Config();
    }
}

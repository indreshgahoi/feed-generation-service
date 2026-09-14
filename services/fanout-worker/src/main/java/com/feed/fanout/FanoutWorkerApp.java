package com.feed.fanout;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.feed.fanout.domain.PostCreatedEvent;
import com.feed.fanout.service.FanoutService;
import com.feed.fanout.storage.http.ColdTierHttpClient;
import com.feed.fanout.storage.neo4j.Neo4jSocialGraphRepository;
import com.feed.fanout.storage.postgres.PostgresActivityRepository;
import com.feed.fanout.storage.postgres.ShardedDataSource;
import com.feed.fanout.storage.redis.RedisHotInboxRepository;
import com.feed.sharding.ShardTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Composition root: loads config, constructs every concrete dependency
 * (sharded Postgres, Neo4j, Redis, the cold-tier HTTP client), wires them
 * into FanoutService, and runs the Kafka consumer loop. See
 * post-ingestion-service/cmd/server/main.go for the same convention in
 * Go -- nothing outside this class knows about Kafka, JDBC, Jedis, or
 * the Neo4j driver directly.
 */
public class FanoutWorkerApp {
    private static final Logger log = LoggerFactory.getLogger(FanoutWorkerApp.class);
    private static final String TOPIC = "post-created";

    public static void main(String[] args) throws Exception {
        Config cfg = Config.load();
        ObjectMapper mapper = new ObjectMapper();

        ShardTopology topology = ShardTopology.load(cfg.shardConfigPath);
        ShardedDataSource dataSource = new ShardedDataSource(topology);
        log.info("loaded shard topology: {} shards", dataSource.numShards());

        try (Neo4jSocialGraphRepository socialGraph =
                     new Neo4jSocialGraphRepository(cfg.neo4jUri, cfg.neo4jUsername, cfg.neo4jPassword);
             JedisPool jedisPool = new JedisPool(cfg.redisHost, cfg.redisPort)) {

            socialGraph.verifyConnectivity();
            log.info("connected to Neo4j graph database");

            PostgresActivityRepository activity = new PostgresActivityRepository(dataSource);
            RedisHotInboxRepository hotInbox = new RedisHotInboxRepository(jedisPool);
            ColdTierHttpClient coldTier = new ColdTierHttpClient(cfg.feedAggregationServiceUrl);

            FanoutService fanoutService = new FanoutService(
                    socialGraph, activity, hotInbox, coldTier, cfg.feedInboxMaxItems, cfg.activeWithinDays);

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBrokers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "fanout-worker");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of(TOPIC));
                log.info("fanout-worker subscribed to '{}'", TOPIC);

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            PostCreatedEvent event = mapper.readValue(record.value(), PostCreatedEvent.class);
                            fanoutService.handle(event);
                        } catch (Exception e) {
                            log.error("failed to process record at offset {}: {}", record.offset(), e.getMessage(), e);
                        }
                    }
                    if (!records.isEmpty()) {
                        consumer.commitSync();
                    }
                }
            }
        }
    }
}

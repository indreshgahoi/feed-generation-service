package com.feed.fanout;

import com.feed.fanout.domain.PostCreatedEvent;
import com.feed.fanout.service.FanoutService;
import com.feed.fanout.storage.grpc.ColdTierGrpcClient;
import com.feed.fanout.storage.neo4j.Neo4jSocialGraphRepository;
import com.feed.fanout.storage.postgres.PostgresActivityRepository;
import com.feed.fanout.storage.postgres.ShardedDataSource;
import com.feed.fanout.storage.redis.RedisHotInboxRepository;
import com.feed.sharding.ShardTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

// feed.events.PostCreatedEvent (the generated FlatBuffers accessor) is
// referenced by its fully-qualified name below rather than imported --
// Java has no import aliasing, and its simple name collides with this
// package's own domain.PostCreatedEvent (the plain POJO FanoutService
// works with; see that class for why the wire format stays out of the
// service layer).

/**
 * Composition root: loads config, constructs every concrete dependency
 * (sharded Postgres, Neo4j, Redis, the cold-tier gRPC client), wires them
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

        ShardTopology topology = ShardTopology.load(cfg.shardConfigPath);
        ShardedDataSource dataSource = new ShardedDataSource(topology);
        log.info("loaded shard topology: {} shards", dataSource.numShards());

        try (Neo4jSocialGraphRepository socialGraph =
                     new Neo4jSocialGraphRepository(cfg.neo4jUri, cfg.neo4jUsername, cfg.neo4jPassword);
             JedisPool jedisPool = new JedisPool(cfg.redisHost, cfg.redisPort);
             ColdTierGrpcClient coldTier = new ColdTierGrpcClient(cfg.feedAggregationServiceGrpcAddr)) {

            socialGraph.verifyConnectivity();
            log.info("connected to Neo4j graph database");

            PostgresActivityRepository activity = new PostgresActivityRepository(dataSource);
            RedisHotInboxRepository hotInbox = new RedisHotInboxRepository(jedisPool);

            FanoutService fanoutService = new FanoutService(
                    socialGraph, activity, hotInbox, coldTier, cfg.feedInboxMaxItems, cfg.activeWithinDays);

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBrokers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "fanout-worker");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            // The post-created event body is a FlatBuffers buffer, not a
            // UTF-8 string -- StringDeserializer would corrupt it. Only
            // the key (author ID, still decimal text) stays a String.
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of(TOPIC));
                log.info("fanout-worker subscribed to '{}'", TOPIC);

                while (true) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(2));
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            fanoutService.handle(decode(record.value()));
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

    /**
     * Decodes the FlatBuffers `post-created` event (schemas/fbs/post_created.fbs)
     * into the plain domain.PostCreatedEvent FanoutService already knows
     * how to handle -- the same POJO boundary the old Jackson path used,
     * so the wire format doesn't leak into the service layer.
     */
    private static PostCreatedEvent decode(byte[] bytes) {
        feed.events.PostCreatedEvent fb = feed.events.PostCreatedEvent.getRootAsPostCreatedEvent(ByteBuffer.wrap(bytes));
        PostCreatedEvent event = new PostCreatedEvent();
        event.postId = fb.postId();
        event.userId = fb.userId();
        event.mediaUrl = fb.mediaUrl();
        event.mediaType = fb.mediaType();
        event.caption = fb.caption();
        event.createdAt = fb.createdAt();
        return event;
    }
}

/**
 * Composition root: loads config, constructs every concrete dependency
 * (sharded Postgres, Redis), wires them into NotificationService, and
 * runs the Kafka consumer loop. Mirrors the same convention as the Go
 * services' cmd/server/main.go and fanout-worker's FanoutWorkerApp.
 */
import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import Redis from 'ioredis';

import type { PostCreatedEvent } from './domain/types.js';
import { NotificationService } from './service/notificationService.js';
import { ShardedPool } from './storage/postgres/shardedPool.js';
import { PostgresNotificationRepository } from './storage/postgres/notificationRepository.js';
import { RedisUsernameDirectory } from './storage/redis/usernameDirectory.js';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const SHARD_CONFIG_PATH = process.env.SHARD_CONFIG_PATH ?? '../../config/shards.json';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const TOPIC = 'post-created';

async function main() {
  const shardedPool = new ShardedPool(SHARD_CONFIG_PATH);
  const redisClient = new Redis(REDIS_URL);

  const usernames = new RedisUsernameDirectory(redisClient);
  const notifications = new PostgresNotificationRepository(shardedPool, shardedPool);
  const service = new NotificationService(usernames, notifications, shardedPool);

  const kafka = new Kafka({
    clientId: 'notification-service',
    brokers: KAFKA_BROKERS,
    logLevel: logLevel.WARN,
  });
  const consumer = kafka.consumer({ groupId: 'notification-service' });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  console.log(`notification-service subscribed to '${TOPIC}'`);

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message, partition }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString()) as PostCreatedEvent;

      try {
        const notified = await service.handlePostCreated(event);
        for (const recipientId of notified) {
          console.log(`notified user ${recipientId} of mention by ${event.userId} in post ${event.postId}`);
        }
        await consumer.commitOffsets([
          { topic: TOPIC, partition, offset: (BigInt(message.offset) + 1n).toString() },
        ]);
      } catch (err) {
        console.error(`failed to process post ${event.postId}:`, err);
      }
    },
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

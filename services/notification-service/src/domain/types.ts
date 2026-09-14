// Domain types and port interfaces only -- no pg, no ioredis, no Kafka
// imports here. See post-ingestion-service/internal/domain for the same
// convention in Go; this is the right-sized Node/TS equivalent for a
// service this small (one use case: react to a post-created event).

export interface PostCreatedEvent {
  postId: string;
  userId: string;
  mediaUrl: string;
  mediaType: number;
  caption: string;
  createdAt: string;
}

/** Backed by the Redis directory post-ingestion-service writes at signup
 * -- see doc/sharding.md "The username problem (a global secondary
 * index)". Usernames aren't the shard key, so there's no bit-shift
 * shortcut to find which of the 4 Postgres shards a username lives on. */
export interface UsernameDirectory {
  lookup(username: string): Promise<bigint | null>;
}

/** Backed by sharded Postgres, co-located with the RECIPIENT's shard. */
export interface NotificationRepository {
  create(recipientId: bigint, actorId: bigint, postId: bigint): Promise<void>;
}

/** The service layer's only view of sharding: mint a self-routing ID
 * inheriting an existing entity's shard. See doc/sharding.md. */
export interface IdMinter {
  newIdInheritingShard(existingId: bigint): bigint;
}

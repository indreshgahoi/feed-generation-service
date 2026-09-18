// Domain types and port interfaces only -- no pg, no ioredis, no Kafka
// imports here. See post-ingestion-service/internal/domain for the same
// convention in Go; this is the right-sized Node/TS equivalent for a
// service this small (one use case: react to a post-created event).

// postId/userId are bigint and createdAt is unix epoch millis -- matching
// schemas/fbs/post_created.fbs directly, since the wire format is
// FlatBuffers, not JSON (see doc/DESIGN.md). This is a better fit
// than the old JSON path's stringified IDs: this service already mints
// its own bigint Snowflake IDs (see IdMinter below), so decoding straight
// into bigint removes a string<->bigint conversion instead of adding one.
export interface PostCreatedEvent {
  postId: bigint;
  userId: bigint;
  mediaUrl: string;
  mediaType: number;
  caption: string;
  createdAt: number;
}

/** Backed by the Redis directory post-ingestion-service writes at signup
 * -- see doc/DESIGN.md "The username problem (a global secondary
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
 * inheriting an existing entity's shard. See doc/DESIGN.md. */
export interface IdMinter {
  newIdInheritingShard(existingId: bigint): bigint;
}

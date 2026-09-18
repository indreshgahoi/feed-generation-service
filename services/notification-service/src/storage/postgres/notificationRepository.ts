import type { IdMinter, NotificationRepository } from '../../domain/types.js';
import type { ShardedPool } from './shardedPool.js';

/** Notifications are co-located with the RECIPIENT's shard -- see
 * doc/DESIGN.md. notification_id is an app-generated self-routing ID
 * (not a DB SERIAL -- see db/shard-schema.sql's header comment on why an
 * auto-increment sequence can't be used once there are 4 independent
 * Postgres instances). */
export class PostgresNotificationRepository implements NotificationRepository {
  constructor(
    private readonly pool: ShardedPool,
    private readonly minter: IdMinter,
  ) {}

  async create(recipientId: bigint, actorId: bigint, postId: bigint): Promise<void> {
    const notificationId = this.minter.newIdInheritingShard(recipientId);
    const client = this.pool.poolForExistingId(recipientId);
    await client.query(
      `INSERT INTO notifications (notification_id, recipient_user_id, actor_user_id, post_id, type)
       VALUES ($1, $2, $3, $4, 'mention')`,
      [notificationId.toString(), recipientId.toString(), actorId.toString(), postId.toString()],
    );
  }
}

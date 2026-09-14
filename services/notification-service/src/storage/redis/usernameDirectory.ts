import Redis from 'ioredis';

import type { UsernameDirectory } from '../../domain/types.js';

/** Reads the same username -> user_id directory post-ingestion-service
 * writes at signup. See doc/sharding.md "The username problem (a global
 * secondary index)" -- username isn't the shard key, so this is the
 * fast path instead of a 4-way Postgres fan-out on every mention. */
export class RedisUsernameDirectory implements UsernameDirectory {
  constructor(private readonly client: Redis) {}

  async lookup(username: string): Promise<bigint | null> {
    const value = await this.client.get(`username:${username}`);
    if (value === null) return null;
    return BigInt(value);
  }
}

import { readFileSync } from 'node:fs';
import pg from 'pg';

import { extractShardId, SnowflakeIdGenerator } from '../sharding/snowflake.js';
import type { IdMinter } from '../../domain/types.js';

interface ShardConfigEntry {
  id: number;
  postgresUrl: string;
}

interface ShardsFile {
  shards: ShardConfigEntry[];
}

/** One pg.Pool per physical shard -- the Node-side equivalent of the Go
 * services' ShardedPool. See doc/sharding.md. */
export class ShardedPool implements IdMinter {
  private readonly pools = new Map<number, pg.Pool>();
  private readonly generators = new Map<number, SnowflakeIdGenerator>();

  constructor(shardConfigPath: string) {
    const config: ShardsFile = JSON.parse(readFileSync(shardConfigPath, 'utf-8'));
    for (const shard of config.shards) {
      this.pools.set(shard.id, new pg.Pool({ connectionString: shard.postgresUrl }));
      this.generators.set(shard.id, new SnowflakeIdGenerator(shard.id));
    }
  }

  /** Routes to the shard that minted id -- a bit-shift, never a lookup.
   * Throws for an ID outside the configured topology (malformed/forged
   * ID) rather than silently returning undefined -- see the Go services'
   * PoolForShard for why this must be a hard error, not a crash later. */
  poolForExistingId(id: bigint): pg.Pool {
    const shardId = extractShardId(id);
    const pool = this.pools.get(shardId);
    if (!pool) {
      throw new Error(`no shard configured with id ${shardId} (have ${this.pools.size} shards)`);
    }
    return pool;
  }

  newIdInheritingShard(existingId: bigint): bigint {
    const shardId = extractShardId(existingId);
    const gen = this.generators.get(shardId);
    if (!gen) {
      throw new Error(`no shard configured with id ${shardId} (have ${this.generators.size} shards)`);
    }
    return gen.next();
  }

  async close(): Promise<void> {
    await Promise.all([...this.pools.values()].map((p) => p.end()));
  }
}

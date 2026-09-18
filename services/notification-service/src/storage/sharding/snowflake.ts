// Mirrors pkg/sharding's Go IDGenerator and com.feed.sharding's Java
// SnowflakeIdGenerator bit-for-bit: 41 bits timestamp | 8 bits shard ID |
// 14 bits sequence, same epoch. MUST use BigInt throughout -- these IDs
// exceed Number.MAX_SAFE_INTEGER (2^53-1), and silently doing this
// arithmetic in `number` would corrupt the low bits (the sequence and
// part of the shard ID) on every single ID. See doc/DESIGN.md.
const EPOCH_MILLIS = 1704067200000n; // 2024-01-01T00:00:00Z
const SEQUENCE_BITS = 14n;
const SHARD_ID_BITS = 8n;
const SHARD_ID_SHIFT = SEQUENCE_BITS;
const TIMESTAMP_SHIFT = SEQUENCE_BITS + SHARD_ID_BITS;
const MAX_SHARD_ID = (1n << SHARD_ID_BITS) - 1n;
const MAX_SEQUENCE = (1n << SEQUENCE_BITS) - 1n;

export class SnowflakeIdGenerator {
  private sequence = 0n;
  private lastTimestamp = -1n;

  constructor(private readonly shardId: number) {
    if (shardId < 0 || BigInt(shardId) > MAX_SHARD_ID) {
      throw new Error(`sharding: shardId out of range for an 8-bit ID field: ${shardId}`);
    }
  }

  next(): bigint {
    let ts = BigInt(Date.now()) - EPOCH_MILLIS;
    if (ts === this.lastTimestamp) {
      this.sequence = (this.sequence + 1n) & MAX_SEQUENCE;
      if (this.sequence === 0n) {
        while (ts <= this.lastTimestamp) {
          ts = BigInt(Date.now()) - EPOCH_MILLIS;
        }
      }
    } else {
      this.sequence = 0n;
    }
    this.lastTimestamp = ts;

    return (ts << TIMESTAMP_SHIFT) | (BigInt(this.shardId) << SHARD_ID_SHIFT) | this.sequence;
  }
}

/** Recovers the shard that minted id -- a bit-shift, never a lookup. */
export function extractShardId(id: bigint): number {
  return Number((id >> SHARD_ID_SHIFT) & MAX_SHARD_ID);
}

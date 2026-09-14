// Package postgres implements domain repositories against the sharded
// relational tier -- read-only from this service's perspective (writes
// happen in post-ingestion-service). See doc/sharding.md.
package postgres

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"

	"sharding"
)

type ShardedPool struct {
	pools map[int]*pgxpool.Pool
}

func NewShardedPool(ctx context.Context, cfg sharding.Config) (*ShardedPool, error) {
	sp := &ShardedPool{pools: make(map[int]*pgxpool.Pool, len(cfg.Shards))}
	for _, shard := range cfg.Shards {
		pool, err := pgxpool.New(ctx, shard.PostgresURL)
		if err != nil {
			return nil, fmt.Errorf("shard %d: connect: %w", shard.ID, err)
		}
		if err := pool.Ping(ctx); err != nil {
			return nil, fmt.Errorf("shard %d: ping: %w", shard.ID, err)
		}
		sp.pools[shard.ID] = pool
	}
	return sp, nil
}

func (sp *ShardedPool) Close() {
	for _, pool := range sp.pools {
		pool.Close()
	}
}

// PoolForExistingID routes to the shard that minted id -- a bit-shift,
// never a lookup. See doc/sharding.md. Returns an error rather than a nil
// pool for a shard ID outside the configured topology (e.g. a malformed
// or forged ID) -- a single bad ID must degrade that one request, not
// crash the process for every concurrent caller.
func (sp *ShardedPool) PoolForExistingID(id int64) (*pgxpool.Pool, error) {
	return sp.PoolForShard(sharding.ExtractShardID(id))
}

func (sp *ShardedPool) PoolForShard(shardID int) (*pgxpool.Pool, error) {
	pool, ok := sp.pools[shardID]
	if !ok {
		return nil, fmt.Errorf("no shard configured with id %d (have %d shards)", shardID, len(sp.pools))
	}
	return pool, nil
}

func (sp *ShardedPool) NumShards() int { return len(sp.pools) }

// ParseID and ExtractShardID are small helpers so repositories don't each
// re-derive shard routing from a string ID independently.
func ParseID(id string) (int64, error) {
	return strconv.ParseInt(id, 10, 64)
}

func ExtractShardID(id int64) int {
	return sharding.ExtractShardID(id)
}

// Package postgres implements domain repositories against the sharded
// relational tier. Every repository here routes through ShardedPool --
// no repository talks to a *pgxpool.Pool directly, so shard routing logic
// lives in exactly one place.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"sharding"
)

// ShardedPool owns one connection pool per physical shard and the
// consistent-hash ring used to place brand-new entities. See
// doc/sharding.md.
type ShardedPool struct {
	pools map[int]*pgxpool.Pool
	gens  map[int]*sharding.IDGenerator
	ring  *sharding.Ring
}

func NewShardedPool(ctx context.Context, cfg sharding.Config) (*ShardedPool, error) {
	sp := &ShardedPool{
		pools: make(map[int]*pgxpool.Pool, len(cfg.Shards)),
		gens:  make(map[int]*sharding.IDGenerator, len(cfg.Shards)),
		ring:  sharding.NewRing(cfg),
	}
	for _, shard := range cfg.Shards {
		pool, err := pgxpool.New(ctx, shard.PostgresURL)
		if err != nil {
			return nil, fmt.Errorf("shard %d: connect: %w", shard.ID, err)
		}
		if err := pool.Ping(ctx); err != nil {
			return nil, fmt.Errorf("shard %d: ping: %w", shard.ID, err)
		}
		sp.pools[shard.ID] = pool
		sp.gens[shard.ID] = sharding.NewIDGenerator(shard.ID)
	}
	return sp, nil
}

func (sp *ShardedPool) Close() {
	for _, pool := range sp.pools {
		pool.Close()
	}
}

// PoolForExistingID routes to the shard that minted id -- a bit-shift,
// never a lookup or a network call. See doc/sharding.md. Returns an
// error rather than a nil pool for a shard ID outside the configured
// topology (e.g. a malformed ID from a URL path parameter) -- one bad ID
// must degrade that one request, not crash the process for every
// concurrent caller.
func (sp *ShardedPool) PoolForExistingID(id int64) (*pgxpool.Pool, error) {
	return sp.PoolForShard(sharding.ExtractShardID(id))
}

// PoolForShard returns a specific shard's pool by ID directly (used when
// the shard is already known, e.g. a post inheriting its author's shard).
func (sp *ShardedPool) PoolForShard(shardID int) (*pgxpool.Pool, error) {
	pool, ok := sp.pools[shardID]
	if !ok {
		return nil, fmt.Errorf("no shard configured with id %d (have %d shards)", shardID, len(sp.pools))
	}
	return pool, nil
}

// NewIDOnShard mints a new self-routing ID on a specific, already-chosen
// shard (e.g. a post inherits its author's shard rather than being
// re-hashed).
func (sp *ShardedPool) NewIDOnShard(shardID int) int64 {
	return sp.gens[shardID].Next()
}

// PlaceNewEntity picks a shard for a brand-new entity via the
// consistent-hash ring and mints its ID on that shard. Used exactly once
// per entity's lifetime: at creation. See doc/sharding.md.
func (sp *ShardedPool) PlaceNewEntity(placementKey string) (shardID int, id int64) {
	shardID = sp.ring.ShardForNewEntity(placementKey)
	return shardID, sp.gens[shardID].Next()
}

// AllPools returns every shard's pool, for explicit cross-shard
// scatter-gather queries. Callers must group by shard and fan out
// concurrently themselves -- this is deliberately not hidden behind
// something that looks like a single query. See doc/sharding.md.
func (sp *ShardedPool) AllPools() map[int]*pgxpool.Pool {
	return sp.pools
}

func (sp *ShardedPool) NumShards() int {
	return sp.ring.NumShards()
}

// ExtractShardID is re-exported so callers building cross-shard fan-out
// (grouping IDs by shard) don't need to import pkg/sharding directly.
func ExtractShardID(id int64) int {
	return sharding.ExtractShardID(id)
}

// NewIDInheritingShard and NewIDForNewEntity implement domain.IDMinter,
// so the service layer depends only on that interface, never on
// pkg/sharding or ShardedPool directly.

func (sp *ShardedPool) NewIDInheritingShard(existingID int64) int64 {
	return sp.NewIDOnShard(sharding.ExtractShardID(existingID))
}

func (sp *ShardedPool) NewIDForNewEntity(placementKey string) int64 {
	_, id := sp.PlaceNewEntity(placementKey)
	return id
}

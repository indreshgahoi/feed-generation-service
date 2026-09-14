// Package redis implements the Redis-backed domain repositories: hot
// engagement counters, the read-your-own-writes like-state cache, and the
// username directory. See doc/engagement-at-scale.md.
package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// numCounterShards splits each post's like counter across N independent
// Redis keys so a single viral post's like traffic isn't serialized
// through one hot key -- see doc/engagement-at-scale.md "Sharded
// counters." 16 matches the reference design; at this repo's demo scale
// it's overkill for correctness (a single INCR is already atomic and
// fast) but it's the part of the hyperscale design that's cheap to
// actually build and demonstrate, so it's implemented for real rather
// than just described.
const numCounterShards = 16

// sumCacheTTLSeconds bounds how stale a read of the total can be. Reads
// outnumber writes by 50:1+ at hyperscale (doc/engagement-at-scale.md
// §1), so caching the summed total -- even briefly -- turns a 16-key
// MGET into a single GET for the overwhelming majority of reads.
const sumCacheTTLSeconds = 5

type CounterRepo struct {
	client *redis.Client
}

func NewCounterRepo(client *redis.Client) *CounterRepo {
	return &CounterRepo{client: client}
}

func likeShardKey(postID, userID int64) string {
	return fmt.Sprintf("likes:count:%d:%d", postID, counterShard(userID))
}

func likeSumCacheKey(postID int64) string { return fmt.Sprintf("likes:count:%d:sum", postID) }

func commentCountKey(postID int64) string { return fmt.Sprintf("comments:count:%d", postID) }

// counterShard is a plain hash % N, NOT the consistent-hash ring used
// for Postgres shard placement (pkg/sharding) -- these are two unrelated
// sharding schemes solving two different problems (which physical
// Postgres owns a row, vs. which Redis key absorbs one counter
// increment), and conflating them would be a mistake, not a
// simplification. See doc/engagement-at-scale.md.
func counterShard(userID int64) int64 {
	if userID < 0 {
		userID = -userID
	}
	return userID % numCounterShards
}

// IncrLikeCount writes to the sub-counter owned by the LIKING user's
// hash, not a single per-post key -- see doc/engagement-at-scale.md
// "The bottleneck: row-lock contention" (the Redis analogue: one hot key
// per viral post instead of one hot row).
func (r *CounterRepo) IncrLikeCount(ctx context.Context, postID, userID int64) (int64, error) {
	if _, err := r.client.Incr(ctx, likeShardKey(postID, userID)).Result(); err != nil {
		return 0, err
	}
	r.client.Del(ctx, likeSumCacheKey(postID)) // invalidate the cached sum
	return r.GetLikeCount(ctx, postID)
}

func (r *CounterRepo) DecrLikeCount(ctx context.Context, postID, userID int64) (int64, error) {
	key := likeShardKey(postID, userID)
	count, err := r.client.Decr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if count < 0 {
		// Defensive: a decrement racing ahead of its increment on this
		// SAME sub-shard should never leave a negative sub-count visible.
		r.client.Set(ctx, key, 0, 0)
	}
	r.client.Del(ctx, likeSumCacheKey(postID))
	return r.GetLikeCount(ctx, postID)
}

func (r *CounterRepo) IncrCommentCount(ctx context.Context, postID int64) (int64, error) {
	return r.client.Incr(ctx, commentCountKey(postID)).Result()
}

// GetLikeCount serves the cached sum when available (the common case,
// given the read:write ratio this is designed around) and recomputes by
// summing all N sub-shards on a cache miss. See doc/engagement-at-scale.md.
func (r *CounterRepo) GetLikeCount(ctx context.Context, postID int64) (int64, error) {
	if cached, err := r.client.Get(ctx, likeSumCacheKey(postID)).Int64(); err == nil {
		return cached, nil
	}

	keys := make([]string, numCounterShards)
	for i := 0; i < numCounterShards; i++ {
		keys[i] = fmt.Sprintf("likes:count:%d:%d", postID, i)
	}
	values, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return 0, err
	}

	var total int64
	for _, v := range values {
		if v == nil {
			continue
		}
		if s, ok := v.(string); ok {
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				total += n
			}
		}
	}

	r.client.Set(ctx, likeSumCacheKey(postID), total, sumCacheTTLSeconds*time.Second)
	return total, nil
}

func (r *CounterRepo) GetCommentCount(ctx context.Context, postID int64) (int64, error) {
	val, err := r.client.Get(ctx, commentCountKey(postID)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

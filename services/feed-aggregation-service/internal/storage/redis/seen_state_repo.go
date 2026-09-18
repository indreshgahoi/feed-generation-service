package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// SeenStateRepo implements the "Seen-State Check (drops posts viewed in
// last 48h)" stage as a Redis ZSET keyed by last-shown timestamp. See
// doc/TRADE-OFFS.md for why a bounded-TTL ZSET approximates a Bloom
// filter here.
type SeenStateRepo struct {
	client *goredis.Client
}

func NewSeenStateRepo(client *goredis.Client) *SeenStateRepo {
	return &SeenStateRepo{client: client}
}

func seenKey(userID string) string { return "seen:user:" + userID }

func (r *SeenStateRepo) FilterUnseen(ctx context.Context, userID string, postIDs []string, ttlSeconds int) ([]string, error) {
	if len(postIDs) == 0 {
		return nil, nil
	}
	key := seenKey(userID)

	pipe := r.client.Pipeline()
	cmds := make(map[string]*goredis.FloatCmd, len(postIDs))
	for _, id := range postIDs {
		cmds[id] = pipe.ZScore(ctx, key, id)
	}
	_, _ = pipe.Exec(ctx) // ZScore returns redis.Nil for missing members; not a real error

	cutoff := float64(time.Now().Add(-time.Duration(ttlSeconds) * time.Second).UnixMilli())
	unseen := make([]string, 0, len(postIDs))
	for _, id := range postIDs {
		score, err := cmds[id].Result()
		if err == goredis.Nil || score < cutoff {
			unseen = append(unseen, id)
		}
	}
	return unseen, nil
}

func (r *SeenStateRepo) MarkSeen(ctx context.Context, userID string, postIDs []string) error {
	if len(postIDs) == 0 {
		return nil
	}
	key := seenKey(userID)
	now := float64(time.Now().UnixMilli())

	pipe := r.client.Pipeline()
	for _, id := range postIDs {
		pipe.ZAdd(ctx, key, goredis.Z{Score: now, Member: id})
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Reset is a LOCAL DEMO CONVENIENCE ONLY: it exists because this repo's
// sample content pool is small and fixed, so paging through it once
// exhausts what's unseen. A real feed never needs this. See README.md.
func (r *SeenStateRepo) Reset(ctx context.Context, userID string) error {
	return r.client.Del(ctx, seenKey(userID)).Err()
}

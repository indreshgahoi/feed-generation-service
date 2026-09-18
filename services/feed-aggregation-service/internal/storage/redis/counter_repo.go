package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// CounterRepo reads the engagement counters post-ingestion-service
// writes on like/comment. See doc/DESIGN.md "Counters live in Redis,
// not the sharded database".
type CounterRepo struct {
	client *goredis.Client
}

func NewCounterRepo(client *goredis.Client) *CounterRepo {
	return &CounterRepo{client: client}
}

func (r *CounterRepo) BatchGetCounts(ctx context.Context, postIDs []string) (map[string]int64, map[string]int64, error) {
	likes := make(map[string]int64, len(postIDs))
	comments := make(map[string]int64, len(postIDs))
	if len(postIDs) == 0 {
		return likes, comments, nil
	}

	pipe := r.client.Pipeline()
	likeCmds := make(map[string]*goredis.StringCmd, len(postIDs))
	commentCmds := make(map[string]*goredis.StringCmd, len(postIDs))
	for _, id := range postIDs {
		likeCmds[id] = pipe.Get(ctx, "likes:count:"+id)
		commentCmds[id] = pipe.Get(ctx, "comments:count:"+id)
	}
	_, _ = pipe.Exec(ctx) // Get returns redis.Nil for unset counters; not a real error

	for _, id := range postIDs {
		likes[id] = parseCountOrZero(likeCmds[id])
		comments[id] = parseCountOrZero(commentCmds[id])
	}
	return likes, comments, nil
}

func parseCountOrZero(cmd *goredis.StringCmd) int64 {
	val, err := cmd.Int64()
	if err != nil {
		return 0
	}
	return val
}

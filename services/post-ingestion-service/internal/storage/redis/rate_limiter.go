package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter is a fixed-window counter: INCR-then-EXPIRE-if-new on a
// key that names both the actor and the time window, so windows expire
// themselves instead of needing a cleanup job. See
// doc/engagement-at-scale.md "Like / Unlike Spam (Flapping)" -- this
// guards against exactly that: a user (or bot) rapidly toggling
// like/unlike on the same post.
type RateLimiter struct {
	client *redis.Client
	limit  int64
	window time.Duration
}

func NewRateLimiter(client *redis.Client, limit int64, window time.Duration) *RateLimiter {
	return &RateLimiter{client: client, limit: limit, window: window}
}

func (r *RateLimiter) Allow(ctx context.Context, key string) (bool, error) {
	windowKey := "ratelimit:" + key
	count, err := r.client.Incr(ctx, windowKey).Result()
	if err != nil {
		return false, err
	}
	if count == 1 {
		// First hit in this window -- start the window's expiry now.
		// A fixed window (vs. a sliding one) means a burst can land just
		// either side of a window boundary and briefly exceed the
		// nominal rate; acceptable here, since this is an abuse
		// backstop, not a precise SLA enforcement.
		r.client.Expire(ctx, windowKey, r.window)
	}
	return count <= r.limit, nil
}

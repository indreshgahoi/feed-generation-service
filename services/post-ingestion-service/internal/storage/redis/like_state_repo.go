package redis

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// LikeStateRepo implements the "read-your-own-writes" cache from
// doc/engagement-at-scale.md: a Redis Set per user, so "does THIS viewer
// like THIS post" never has to touch sharded Postgres or wait on
// cross-shard anything. The client renders the heart icon filled purely
// from this, immediately after a successful Like call.
type LikeStateRepo struct {
	client *redis.Client
}

func NewLikeStateRepo(client *redis.Client) *LikeStateRepo {
	return &LikeStateRepo{client: client}
}

func userLikesKey(userID int64) string {
	return "user_likes:" + strconv.FormatInt(userID, 10)
}

func (r *LikeStateRepo) MarkLiked(ctx context.Context, userID, postID int64) error {
	return r.client.SAdd(ctx, userLikesKey(userID), postID).Err()
}

func (r *LikeStateRepo) MarkUnliked(ctx context.Context, userID, postID int64) error {
	return r.client.SRem(ctx, userLikesKey(userID), postID).Err()
}

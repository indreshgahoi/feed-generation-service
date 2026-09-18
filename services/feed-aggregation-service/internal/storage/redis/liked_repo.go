package redis

import (
	"context"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
)

// LikedRepo answers "did this viewer like these posts" from the same
// Redis Set post-ingestion-service writes on every like/unlike --
// user_likes:<userID> -- rather than querying sharded Postgres. See
// doc/DESIGN.md: this is the read-your-own-writes cache,
// and it's meant to be the fast path for exactly this kind of batch
// check during feed hydration, not a fallback.
type LikedRepo struct {
	client *goredis.Client
}

func NewLikedRepo(client *goredis.Client) *LikedRepo {
	return &LikedRepo{client: client}
}

func (r *LikedRepo) BatchIsLiked(ctx context.Context, viewerID string, postIDs []string) (map[string]bool, error) {
	result := make(map[string]bool, len(postIDs))
	if len(postIDs) == 0 {
		return result, nil
	}

	key := "user_likes:" + viewerID
	pipe := r.client.Pipeline()
	cmds := make(map[string]*goredis.BoolCmd, len(postIDs))
	for _, postID := range postIDs {
		if id, err := strconv.ParseInt(postID, 10, 64); err == nil {
			cmds[postID] = pipe.SIsMember(ctx, key, id)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, err
	}

	for postID, cmd := range cmds {
		result[postID] = cmd.Val()
	}
	return result, nil
}

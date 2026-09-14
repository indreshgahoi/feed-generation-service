package postgres

import (
	"context"

	"post-ingestion-service/internal/domain"
)

// LikeRepo stores like rows on the LIKING user's shard, not the post's --
// see doc/sharding.md. like_count itself is NOT here; it lives in Redis
// (internal/storage/redis.CounterRepo) specifically because a like's
// author and the post's author are frequently on different shards, and
// incrementing a counter co-located with the post would make every like
// a cross-shard write.
type LikeRepo struct {
	pool *ShardedPool
}

func NewLikeRepo(pool *ShardedPool) *LikeRepo {
	return &LikeRepo{pool: pool}
}

func (r *LikeRepo) Create(ctx context.Context, like domain.Like) (bool, error) {
	pool, err := r.pool.PoolForExistingID(like.UserID)
	if err != nil {
		return false, err
	}
	tag, err := pool.Exec(ctx,
		`INSERT INTO likes (post_id, user_id, created_at) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		like.PostID, like.UserID, like.CreatedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (r *LikeRepo) Delete(ctx context.Context, postID, userID int64) (bool, error) {
	pool, err := r.pool.PoolForExistingID(userID)
	if err != nil {
		return false, err
	}
	tag, err := pool.Exec(ctx, `DELETE FROM likes WHERE post_id = $1 AND user_id = $2`, postID, userID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

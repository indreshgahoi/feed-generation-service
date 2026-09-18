package postgres

import (
	"context"

	"post-ingestion-service/internal/domain"
)

// CommentRepo stores comment rows on the POST's shard, not the
// commenter's -- see doc/DESIGN.md. comment_id is minted on the post's
// shard (the caller passes an ID already placed there; see
// service.EngagementService).
type CommentRepo struct {
	pool *ShardedPool
}

func NewCommentRepo(pool *ShardedPool) *CommentRepo {
	return &CommentRepo{pool: pool}
}

func (r *CommentRepo) Create(ctx context.Context, comment domain.Comment) error {
	pool, err := r.pool.PoolForExistingID(comment.CommentID)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx,
		`INSERT INTO comments (comment_id, post_id, user_id, body, created_at) VALUES ($1, $2, $3, $4, $5)`,
		comment.CommentID, comment.PostID, comment.UserID, comment.Body, comment.CreatedAt)
	return err
}

func (r *CommentRepo) ListByPost(ctx context.Context, postID int64) ([]domain.Comment, error) {
	// Comments are co-located with the POST's shard, so postID itself
	// (not any commenter's ID) tells us which shard to query.
	pool, err := r.pool.PoolForExistingID(postID)
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
		SELECT c.comment_id, c.post_id, c.user_id, c.body, c.created_at
		FROM comments c
		WHERE c.post_id = $1
		ORDER BY c.created_at ASC
	`, postID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var comments []domain.Comment
	for rows.Next() {
		var c domain.Comment
		if err := rows.Scan(&c.CommentID, &c.PostID, &c.UserID, &c.Body, &c.CreatedAt); err != nil {
			return nil, err
		}
		comments = append(comments, c)
	}
	return comments, rows.Err()
}

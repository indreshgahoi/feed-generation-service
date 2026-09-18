package postgres

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"post-ingestion-service/internal/domain"
)

type PostRepo struct {
	pool *ShardedPool
}

func NewPostRepo(pool *ShardedPool) *PostRepo {
	return &PostRepo{pool: pool}
}

func (r *PostRepo) Create(ctx context.Context, post domain.Post) error {
	pool, err := r.pool.PoolForExistingID(post.PostID)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx,
		`INSERT INTO posts (post_id, user_id, media_url, media_type, caption, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		post.PostID, post.UserID, post.MediaURL, post.MediaType, post.Caption, post.CreatedAt)
	return err
}

// ListRecentByAuthors groups userIDs by shard (bit-shift, no lookup),
// fans out to each relevant shard concurrently, and merges by CreatedAt.
// See doc/DESIGN.md -- this is the one cross-shard scatter-gather a
// graph database doesn't remove, because it's about post storage, not
// the social graph.
func (r *PostRepo) ListRecentByAuthors(ctx context.Context, userIDs []int64, limitPerAuthor int) ([]domain.Post, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	byShard := make(map[int][]int64)
	for _, id := range userIDs {
		shardID := ExtractShardID(id)
		if _, err := r.pool.PoolForShard(shardID); err != nil {
			// A malformed/forged ID resolves to a shard outside our
			// topology -- drop just this ID rather than failing the
			// whole batch. See pool.go's PoolForShard doc comment.
			continue
		}
		byShard[shardID] = append(byShard[shardID], id)
	}

	type result struct {
		posts []domain.Post
		err   error
	}
	results := make(chan result, len(byShard))
	var wg sync.WaitGroup

	for shardID, ids := range byShard {
		wg.Add(1)
		go func(shardID int, ids []int64) {
			defer wg.Done()
			pool, err := r.pool.PoolForShard(shardID) // already validated above
			if err != nil {
				results <- result{err: err}
				return
			}
			posts, err := queryRecentPosts(ctx, pool, ids, limitPerAuthor)
			if err != nil {
				results <- result{err: fmt.Errorf("shard %d: %w", shardID, err)}
				return
			}
			results <- result{posts: posts}
		}(shardID, ids)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var (
		all      []domain.Post
		firstErr error
	)
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		all = append(all, res.posts...)
	}
	if firstErr != nil {
		return nil, firstErr
	}

	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt.After(all[j].CreatedAt) })
	return all, nil
}

func queryRecentPosts(ctx context.Context, pool *pgxpool.Pool, userIDs []int64, limitPerAuthor int) ([]domain.Post, error) {
	rows, err := pool.Query(ctx, `
		SELECT post_id, user_id, media_url, media_type, caption, created_at
		FROM posts
		WHERE user_id = ANY($1)
		ORDER BY created_at DESC
		LIMIT $2
	`, userIDs, limitPerAuthor*len(userIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var posts []domain.Post
	for rows.Next() {
		var p domain.Post
		var caption *string
		if err := rows.Scan(&p.PostID, &p.UserID, &p.MediaURL, &p.MediaType, &caption, &p.CreatedAt); err != nil {
			return nil, err
		}
		if caption != nil {
			p.Caption = *caption
		}
		posts = append(posts, p)
	}
	return posts, rows.Err()
}

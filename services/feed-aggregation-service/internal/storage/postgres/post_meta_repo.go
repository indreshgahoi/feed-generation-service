package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"feed-aggregation-service/internal/domain"
)

type PostMetaRepo struct {
	pool *ShardedPool
}

func NewPostMetaRepo(pool *ShardedPool) *PostMetaRepo {
	return &PostMetaRepo{pool: pool}
}

// groupByShard buckets IDs by their embedded shard, silently dropping any
// ID whose shard isn't part of the configured topology (a malformed or
// forged ID) rather than propagating that as an error for the whole
// batch: one bad candidate ID should make that one candidate disappear
// from the feed, not fail every other candidate's lookup or crash the
// process. This is what PoolForShard's error return is FOR -- see its
// doc comment in pool.go.
func (r *PostMetaRepo) groupByShard(ids []string) map[int][]int64 {
	byShard := make(map[int][]int64)
	for _, idStr := range ids {
		id, err := ParseID(idStr)
		if err != nil {
			continue
		}
		shardID := ExtractShardID(id)
		if _, err := r.pool.PoolForShard(shardID); err != nil {
			slog.Warn("dropping candidate with out-of-range shard ID", "id", idStr, "shardID", shardID, "error", err)
			continue
		}
		byShard[shardID] = append(byShard[shardID], id)
	}
	return byShard
}

// GetBatch groups postIDs by shard (bit-shift, no lookup), fans out
// concurrently, and merges. Note this does NOT return like/comment
// counts -- those live in Redis; see doc/DESIGN.md.
func (r *PostMetaRepo) GetBatch(ctx context.Context, postIDs []string) (map[string]domain.PostMeta, error) {
	byShard := r.groupByShard(postIDs)

	type result struct {
		metas []domain.PostMeta
		err   error
	}
	results := make(chan result, len(byShard))
	var wg sync.WaitGroup

	for shardID, ids := range byShard {
		wg.Add(1)
		go func(shardID int, ids []int64) {
			defer wg.Done()
			pool, err := r.pool.PoolForShard(shardID) // already validated in groupByShard
			if err != nil {
				results <- result{err: err}
				return
			}
			metas, err := queryPostMeta(ctx, pool, ids)
			if err != nil {
				results <- result{err: fmt.Errorf("shard %d: %w", shardID, err)}
				return
			}
			results <- result{metas: metas}
		}(shardID, ids)
	}
	go func() { wg.Wait(); close(results) }()

	out := make(map[string]domain.PostMeta)
	var firstErr error
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		for _, m := range res.metas {
			out[m.PostID] = m
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func queryPostMeta(ctx context.Context, pool *pgxpool.Pool, postIDs []int64) ([]domain.PostMeta, error) {
	rows, err := pool.Query(ctx, `
		SELECT post_id, user_id, media_url, caption, created_at
		FROM posts
		WHERE post_id = ANY($1)
	`, postIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metas []domain.PostMeta
	for rows.Next() {
		var (
			postID, userID int64
			caption        *string
			m              domain.PostMeta
		)
		if err := rows.Scan(&postID, &userID, &m.MediaURL, &caption, &m.CreatedAt); err != nil {
			return nil, err
		}
		m.PostID = fmt.Sprintf("%d", postID)
		m.AuthorID = fmt.Sprintf("%d", userID)
		if caption != nil {
			m.Caption = *caption
		}
		metas = append(metas, m)
	}
	return metas, rows.Err()
}

// RecentByAuthors groups authorIDs by shard, fans out in parallel, and
// merges by CreatedAt. See doc/DESIGN.md -- the one cross-shard
// scatter-gather a graph database doesn't remove, because it's about
// post storage, not the social graph.
func (r *PostMetaRepo) RecentByAuthors(ctx context.Context, authorIDs []string, limitPerAuthor int) ([]domain.PostMeta, error) {
	byShard := r.groupByShard(authorIDs)
	if len(byShard) == 0 {
		return nil, nil
	}

	type result struct {
		metas []domain.PostMeta
		err   error
	}
	results := make(chan result, len(byShard))
	var wg sync.WaitGroup

	for shardID, ids := range byShard {
		wg.Add(1)
		go func(shardID int, ids []int64) {
			defer wg.Done()
			pool, err := r.pool.PoolForShard(shardID) // already validated in groupByShard
			if err != nil {
				results <- result{err: err}
				return
			}
			metas, err := queryRecentPosts(ctx, pool, ids, limitPerAuthor)
			if err != nil {
				results <- result{err: fmt.Errorf("shard %d: %w", shardID, err)}
				return
			}
			results <- result{metas: metas}
		}(shardID, ids)
	}
	go func() { wg.Wait(); close(results) }()

	var all []domain.PostMeta
	var firstErr error
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		all = append(all, res.metas...)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return all, nil
}

func queryRecentPosts(ctx context.Context, pool *pgxpool.Pool, authorIDs []int64, limitPerAuthor int) ([]domain.PostMeta, error) {
	rows, err := pool.Query(ctx, `
		SELECT post_id, user_id, media_url, caption, created_at
		FROM posts
		WHERE user_id = ANY($1)
		ORDER BY created_at DESC
		LIMIT $2
	`, authorIDs, limitPerAuthor*len(authorIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metas []domain.PostMeta
	for rows.Next() {
		var (
			postID, userID int64
			caption        *string
			m              domain.PostMeta
		)
		if err := rows.Scan(&postID, &userID, &m.MediaURL, &caption, &m.CreatedAt); err != nil {
			return nil, err
		}
		m.PostID = fmt.Sprintf("%d", postID)
		m.AuthorID = fmt.Sprintf("%d", userID)
		if caption != nil {
			m.Caption = *caption
		}
		metas = append(metas, m)
	}
	return metas, rows.Err()
}

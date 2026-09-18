// Package redis implements the Redis-backed domain repositories: the hot
// inbox tier, seen-state dedupe, and engagement counters. See
// doc/DESIGN.md and doc/DESIGN.md.
package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"

	"feed-aggregation-service/internal/domain"
)

type HotInboxRepo struct {
	client *goredis.Client
}

func NewHotInboxRepo(client *goredis.Client) *HotInboxRepo {
	return &HotInboxRepo{client: client}
}

func inboxKey(userID string) string       { return "feed:user:" + userID }
func outboxKey(celebrityID string) string { return "celebrity:outbox:" + celebrityID }

func (r *HotInboxRepo) GetInbox(ctx context.Context, userID string, limit int) ([]domain.Candidate, error) {
	results, err := r.client.ZRevRangeWithScores(ctx, inboxKey(userID), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	return toCandidates(results, "", "in-network"), nil
}

func (r *HotInboxRepo) GetCelebrityOutbox(ctx context.Context, celebrityID string, limit int) ([]domain.Candidate, error) {
	results, err := r.client.ZRevRangeWithScores(ctx, outboxKey(celebrityID), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	return toCandidates(results, celebrityID, "celebrity"), nil
}

// Exists reports whether userID has a hot-tier inbox key at all. This is
// the signal that distinguishes "active user with an empty-right-now
// inbox" from "dormant user whose data (if any) is in the cold tier" --
// see doc/DESIGN.md.
func (r *HotInboxRepo) Exists(ctx context.Context, userID string) (bool, error) {
	n, err := r.client.Exists(ctx, inboxKey(userID)).Result()
	return n > 0, err
}

// Promote copies cold-tier candidates into a fresh hot-tier inbox. Called
// when a dormant user reads their feed -- a read means they're active
// again now. See doc/DESIGN.md.
func (r *HotInboxRepo) Promote(ctx context.Context, userID string, candidates []domain.Candidate) error {
	if len(candidates) == 0 {
		return nil
	}
	pipe := r.client.Pipeline()
	key := inboxKey(userID)
	for i, c := range candidates {
		// Cold-tier entries don't carry their original score in this
		// simplified promotion path; rank by original list order
		// (most-recent-first, as cold entries are appended in that order).
		pipe.ZAdd(ctx, key, goredis.Z{Score: float64(len(candidates) - i), Member: c.PostID})
	}
	_, err := pipe.Exec(ctx)
	return err
}

func toCandidates(results []goredis.Z, authorID, source string) []domain.Candidate {
	candidates := make([]domain.Candidate, 0, len(results))
	for _, z := range results {
		candidates = append(candidates, domain.Candidate{
			PostID: z.Member.(string), AuthorID: authorID, Source: source,
		})
	}
	return candidates
}

package service

import (
	"context"
	"fmt"

	"post-ingestion-service/internal/domain"
)

// FollowService is a thin orchestration layer over the graph repository
// -- nearly all the real complexity here (bidirectional traversal,
// atomic follower-count/celebrity updates) lives inside Neo4j
// transactions themselves now, which is the whole point of using a graph
// database instead of hand-rolling it across sharded Postgres. See
// doc/sharding.md.
type FollowService struct {
	graph domain.SocialGraphRepository
}

func NewFollowService(graph domain.SocialGraphRepository) *FollowService {
	return &FollowService{graph: graph}
}

func (s *FollowService) Follow(ctx context.Context, followerID, followeeID int64) error {
	if followerID == followeeID {
		return domain.ErrSelfFollow
	}
	if err := s.graph.Follow(ctx, followerID, followeeID); err != nil {
		return fmt.Errorf("follow: %w", err)
	}
	return nil
}

func (s *FollowService) Unfollow(ctx context.Context, followerID, followeeID int64) error {
	if err := s.graph.Unfollow(ctx, followerID, followeeID); err != nil {
		return fmt.Errorf("unfollow: %w", err)
	}
	return nil
}

func (s *FollowService) Following(ctx context.Context, userID int64) ([]int64, error) {
	return s.graph.Following(ctx, userID)
}

// Package grpc exposes the cold tier over gRPC so fanout-worker (Java)
// can append to it -- BadgerDB is a Go-only embedded library, so a
// cross-language write has to go through this service rather than a
// shared library. See doc/DESIGN.md and doc/DESIGN.md.
//
// This replaces the old POST /internal/cold-tier/append HTTP handler:
// same underlying domain.ColdInboxRepository, typed proto request instead
// of a hand-rolled JSON body with stringified int64 IDs.
package grpc

import (
	"context"
	"strconv"

	coldtierpb "feed-aggregation-service/internal/genproto/coldtier"
	"feed-aggregation-service/internal/domain"
)

type ColdTierServer struct {
	coldtierpb.UnimplementedColdTierServiceServer
	coldInbox domain.ColdInboxRepository
}

func NewColdTierServer(coldInbox domain.ColdInboxRepository) *ColdTierServer {
	return &ColdTierServer{coldInbox: coldInbox}
}

// Append is called by fanout-worker when fanning out a post to a DORMANT
// follower, instead of the pre-tiering behavior of simply dropping that
// fan-out write. See doc/DESIGN.md.
func (s *ColdTierServer) Append(ctx context.Context, req *coldtierpb.AppendRequest) (*coldtierpb.AppendResponse, error) {
	userID := strconv.FormatUint(req.GetUserId(), 10)
	postID := strconv.FormatUint(req.GetPostId(), 10)
	authorID := strconv.FormatUint(req.GetAuthorId(), 10)

	if err := s.coldInbox.Append(ctx, userID, domain.Candidate{
		PostID: postID, AuthorID: authorID, Source: "in-network",
	}); err != nil {
		return nil, err
	}
	return &coldtierpb.AppendResponse{Appended: true}, nil
}

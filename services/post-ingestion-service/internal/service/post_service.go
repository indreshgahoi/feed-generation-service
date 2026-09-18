package service

import (
	"context"
	"fmt"
	"time"

	"post-ingestion-service/internal/domain"
)

type PostService struct {
	posts     domain.PostRepository
	publisher domain.EventPublisher
	minter    domain.IDMinter
	clock     func() time.Time
}

func NewPostService(posts domain.PostRepository, publisher domain.EventPublisher, minter domain.IDMinter) *PostService {
	return &PostService{posts: posts, publisher: publisher, minter: minter, clock: time.Now}
}

type CreatePostInput struct {
	AuthorID  int64
	MediaURL  string
	MediaType domain.MediaType
	Caption   string
}

// CreatePost implements the write path from the design doc: mint a
// self-routing ID that inherits the author's shard (a post is never
// re-hashed to its own shard -- see doc/DESIGN.md), commit to the
// author's shard synchronously, then publish asynchronously so fan-out/
// vector/notification consumers can react without the caller waiting on
// any of them.
func (s *PostService) CreatePost(ctx context.Context, in CreatePostInput) (domain.Post, error) {
	if in.MediaURL == "" || !in.MediaType.Valid() {
		return domain.Post{}, fmt.Errorf("%w: mediaUrl and a valid mediaType (1-3) are required", domain.ErrInvalidInput)
	}

	post := domain.Post{
		PostID:    s.minter.NewIDInheritingShard(in.AuthorID),
		UserID:    in.AuthorID,
		MediaURL:  in.MediaURL,
		MediaType: in.MediaType,
		Caption:   in.Caption,
		CreatedAt: s.clock(),
	}

	if err := s.posts.Create(ctx, post); err != nil {
		return domain.Post{}, fmt.Errorf("create post: %w", err)
	}

	if err := s.publisher.PublishPostCreated(ctx, post); err != nil {
		// The Postgres commit already succeeded; a real system would use
		// a transactional outbox here instead of losing the event. See
		// the write path discussion in doc/DESIGN.md.
		return post, fmt.Errorf("post created but event publish failed (fan-out will not run for this post): %w", err)
	}
	return post, nil
}

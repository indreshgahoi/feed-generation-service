package service_test

import (
	"context"
	"errors"
	"testing"

	"post-ingestion-service/internal/domain"
	"post-ingestion-service/internal/service"
)

func TestPostService_CreatePost_InheritsAuthorShard(t *testing.T) {
	posts := &mockPostRepo{}
	publisher := &mockPublisher{}
	minter := newMockIDMinter(500)
	svc := service.NewPostService(posts, publisher, minter)

	const authorID = int64(42)
	post, err := svc.CreatePost(context.Background(), service.CreatePostInput{
		AuthorID: authorID, MediaURL: "https://example.com/a.jpg", MediaType: domain.MediaTypeImage,
	})
	if err != nil {
		t.Fatalf("CreatePost returned error: %v", err)
	}

	if len(minter.inheritCalls) != 1 || minter.inheritCalls[0] != authorID {
		t.Errorf("expected post ID to inherit the AUTHOR's shard (minter.NewIDInheritingShard(%d)), got calls %v", authorID, minter.inheritCalls)
	}
	if len(minter.newEntityCalls) != 0 {
		t.Error("post creation must never make a NEW-entity placement decision -- it inherits, it doesn't get re-hashed (see doc/DESIGN.md)")
	}
	if len(posts.posts) != 1 || posts.posts[0].PostID != post.PostID {
		t.Error("post was not persisted")
	}
	if len(publisher.published) != 1 {
		t.Error("post-created event was not published")
	}
}

func TestPostService_CreatePost_RejectsInvalidMediaType(t *testing.T) {
	svc := service.NewPostService(&mockPostRepo{}, &mockPublisher{}, newMockIDMinter(0))
	_, err := svc.CreatePost(context.Background(), service.CreatePostInput{
		AuthorID: 1, MediaURL: "https://example.com/a.jpg", MediaType: domain.MediaType(9),
	})
	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput for out-of-range media type, got %v", err)
	}
}

func TestPostService_CreatePost_RejectsMissingMediaURL(t *testing.T) {
	svc := service.NewPostService(&mockPostRepo{}, &mockPublisher{}, newMockIDMinter(0))
	_, err := svc.CreatePost(context.Background(), service.CreatePostInput{
		AuthorID: 1, MediaType: domain.MediaTypeImage,
	})
	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput for missing mediaUrl, got %v", err)
	}
}

func TestPostService_CreatePost_ReturnsPostEvenIfPublishFails(t *testing.T) {
	// The Postgres commit is the durability boundary; a Kafka publish
	// failure must not roll back or hide the fact the post was created --
	// see the comment in post_service.go about needing a transactional
	// outbox for this in a real system.
	publisher := &mockPublisher{publishErr: errors.New("kafka unavailable")}
	svc := service.NewPostService(&mockPostRepo{}, publisher, newMockIDMinter(0))

	post, err := svc.CreatePost(context.Background(), service.CreatePostInput{
		AuthorID: 1, MediaURL: "https://example.com/a.jpg", MediaType: domain.MediaTypeImage,
	})
	if err == nil {
		t.Fatal("expected an error surfaced when publish fails")
	}
	if post.PostID == 0 {
		t.Error("expected the created post to still be returned even though publish failed")
	}
}

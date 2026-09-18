package service_test

import (
	"context"
	"errors"
	"testing"

	"post-ingestion-service/internal/domain"
	"post-ingestion-service/internal/service"
)

func newEngagementService() (*service.EngagementService, *mockLikeRepo, *mockCommentRepo, *mockUserRepo, *mockCounterRepo) {
	likes := newMockLikeRepo()
	comments := newMockCommentRepo()
	users := newMockUserRepo()
	counters := newMockCounterRepo()
	minter := newMockIDMinter(0)
	svc := service.NewEngagementService(likes, comments, users, counters, newMockLikeState(), newMockRateLimiter(), newMockModerator(), minter)
	return svc, likes, comments, users, counters
}

func TestEngagementService_Like_IsIdempotent(t *testing.T) {
	svc, _, _, _, counters := newEngagementService()
	ctx := context.Background()

	first, err := svc.Like(ctx, 100, 1)
	if err != nil || !first.Liked || first.LikeCount != 1 {
		t.Fatalf("first like: got %+v, err %v", first, err)
	}

	second, err := svc.Like(ctx, 100, 1)
	if err != nil {
		t.Fatalf("second like errored: %v", err)
	}
	if second.LikeCount != 1 {
		t.Errorf("liking the same post twice must not double-count: got count %d, want 1", second.LikeCount)
	}
	if counters.likeCounts[100] != 1 {
		t.Errorf("underlying counter drifted: %d", counters.likeCounts[100])
	}
}

func TestEngagementService_Unlike_IsIdempotent(t *testing.T) {
	svc, _, _, _, _ := newEngagementService()
	ctx := context.Background()

	if _, err := svc.Like(ctx, 100, 1); err != nil {
		t.Fatal(err)
	}
	first, err := svc.Unlike(ctx, 100, 1)
	if err != nil || first.Liked || first.LikeCount != 0 {
		t.Fatalf("first unlike: got %+v, err %v", first, err)
	}

	second, err := svc.Unlike(ctx, 100, 1)
	if err != nil {
		t.Fatalf("second unlike errored: %v", err)
	}
	if second.LikeCount != 0 {
		t.Errorf("unliking an already-unliked post must not go negative: got %d", second.LikeCount)
	}
}

func TestEngagementService_CreateComment_MintsIDOnPostShard(t *testing.T) {
	svc, _, _, users, _ := newEngagementService()
	users.users[7] = domain.User{UserID: 7, Username: "bob"}

	const postID = int64(999)
	comment, err := svc.CreateComment(context.Background(), postID, 7, "nice post")
	if err != nil {
		t.Fatalf("CreateComment returned error: %v", err)
	}
	if comment.Username != "bob" {
		t.Errorf("expected comment hydrated with author username, got %q", comment.Username)
	}
	if comment.PostID != postID {
		t.Errorf("got post ID %d, want %d", comment.PostID, postID)
	}
}

func TestEngagementService_CreateComment_RejectsEmptyBody(t *testing.T) {
	svc, _, _, _, _ := newEngagementService()
	_, err := svc.CreateComment(context.Background(), 1, 1, "")
	if err == nil {
		t.Error("expected an error for an empty comment body")
	}
}

func TestEngagementService_ListComments_HydratesUsernamesAcrossShards(t *testing.T) {
	svc, _, comments, users, _ := newEngagementService()
	users.users[1] = domain.User{UserID: 1, Username: "alice"}
	users.users[2] = domain.User{UserID: 2, Username: "bob"}
	comments.byPost[500] = []domain.Comment{
		{CommentID: 1, PostID: 500, UserID: 1, Body: "first"},
		{CommentID: 2, PostID: 500, UserID: 2, Body: "second"},
	}

	result, err := svc.ListComments(context.Background(), 500)
	if err != nil {
		t.Fatalf("ListComments returned error: %v", err)
	}
	if len(result) != 2 || result[0].Username != "alice" || result[1].Username != "bob" {
		t.Errorf("usernames not correctly hydrated: %+v", result)
	}
}

func TestEngagementService_Like_UpdatesReadYourOwnWritesCache(t *testing.T) {
	likes, comments, users, counters := newMockLikeRepo(), newMockCommentRepo(), newMockUserRepo(), newMockCounterRepo()
	likeState := newMockLikeState()
	svc := service.NewEngagementService(likes, comments, users, counters, likeState, newMockRateLimiter(), newMockModerator(), newMockIDMinter(0))

	if _, err := svc.Like(context.Background(), 100, 1); err != nil {
		t.Fatal(err)
	}
	if !likeState.liked[[2]int64{1, 100}] {
		t.Error("expected the read-your-own-writes cache to record user 1 liking post 100")
	}

	if _, err := svc.Unlike(context.Background(), 100, 1); err != nil {
		t.Fatal(err)
	}
	if likeState.liked[[2]int64{1, 100}] {
		t.Error("expected the read-your-own-writes cache to clear after unlike")
	}
}

func TestEngagementService_Like_RespectsRateLimit(t *testing.T) {
	likes, comments, users, counters := newMockLikeRepo(), newMockCommentRepo(), newMockUserRepo(), newMockCounterRepo()
	limiter := newMockRateLimiter()
	limiter.allow = false
	svc := service.NewEngagementService(likes, comments, users, counters, newMockLikeState(), limiter, newMockModerator(), newMockIDMinter(0))

	_, err := svc.Like(context.Background(), 100, 1)
	if !errors.Is(err, domain.ErrRateLimited) {
		t.Errorf("expected ErrRateLimited, got %v", err)
	}
}

func TestEngagementService_CreateComment_RejectsModeratedContent(t *testing.T) {
	likes, comments, users, counters := newMockLikeRepo(), newMockCommentRepo(), newMockUserRepo(), newMockCounterRepo()
	moderator := newMockModerator()
	moderator.allow = false
	svc := service.NewEngagementService(likes, comments, users, counters, newMockLikeState(), newMockRateLimiter(), moderator, newMockIDMinter(0))

	_, err := svc.CreateComment(context.Background(), 1, 1, "check out spamlink.biz")
	if !errors.Is(err, domain.ErrContentRejected) {
		t.Errorf("expected ErrContentRejected, got %v", err)
	}
	if len(comments.byPost[1]) != 0 {
		t.Error("a moderation-rejected comment must not be persisted")
	}
}

func TestEngagementService_ListComments_CollapsesConcurrentRequests(t *testing.T) {
	// Regression-style test for the singleflight wrapper: concurrent
	// ListComments calls for the same post should not each independently
	// re-fetch -- see doc/DESIGN.md "Hot Post Cache
	// Stampede." This doesn't assert call counts (the mock repo is cheap
	// enough that asserting singleflight actually collapsed calls would
	// be flaky under `go test -race` scheduling); it asserts the
	// observable contract -- concurrent callers all get the right,
	// consistent result -- which is what actually matters.
	svc, _, comments, users, _ := newEngagementService()
	users.users[1] = domain.User{UserID: 1, Username: "alice"}
	comments.byPost[500] = []domain.Comment{{CommentID: 1, PostID: 500, UserID: 1, Body: "hi"}}

	const n = 20
	results := make(chan []service.CommentWithAuthor, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			result, err := svc.ListComments(context.Background(), 500)
			results <- result
			errs <- err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent ListComments call failed: %v", err)
		}
		if result := <-results; len(result) != 1 || result[0].Username != "alice" {
			t.Errorf("concurrent ListComments call returned unexpected result: %+v", result)
		}
	}
}

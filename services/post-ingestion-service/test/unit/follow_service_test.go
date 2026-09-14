package service_test

import (
	"context"
	"errors"
	"testing"

	"post-ingestion-service/internal/domain"
	"post-ingestion-service/internal/service"
)

func TestFollowService_Follow_RejectsSelfFollow(t *testing.T) {
	svc := service.NewFollowService(newMockGraphRepo())
	err := svc.Follow(context.Background(), 1, 1)
	if !errors.Is(err, domain.ErrSelfFollow) {
		t.Errorf("expected ErrSelfFollow, got %v", err)
	}
}

func TestFollowService_Follow_UpdatesFollowerCountAndCelebrityStatus(t *testing.T) {
	graph := newMockGraphRepo()
	svc := service.NewFollowService(graph)

	// Simulate 25,001 followers to cross the celebrity threshold.
	for i := int64(1); i <= 25001; i++ {
		if err := svc.Follow(context.Background(), i+1000, 1); err != nil {
			t.Fatalf("Follow failed at follower %d: %v", i, err)
		}
	}

	profile, err := graph.GetProfile(context.Background(), 1)
	if err != nil {
		t.Fatalf("GetProfile failed: %v", err)
	}
	if profile.FollowerCount != 25001 {
		t.Errorf("got follower count %d, want 25001", profile.FollowerCount)
	}
	if !profile.IsCelebrity {
		t.Error("expected user to be flagged celebrity after crossing 25,000 followers")
	}
}

func TestFollowService_Unfollow_DecrementsFollowerCount(t *testing.T) {
	graph := newMockGraphRepo()
	svc := service.NewFollowService(graph)

	if err := svc.Follow(context.Background(), 2, 1); err != nil {
		t.Fatal(err)
	}
	if err := svc.Unfollow(context.Background(), 2, 1); err != nil {
		t.Fatal(err)
	}

	profile, _ := graph.GetProfile(context.Background(), 1)
	if profile.FollowerCount != 0 {
		t.Errorf("got follower count %d after unfollow, want 0", profile.FollowerCount)
	}

	following, err := svc.Following(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(following) != 0 {
		t.Errorf("expected empty following list after unfollow, got %v", following)
	}
}

func TestFollowService_Unfollow_NeverGoesNegative(t *testing.T) {
	graph := newMockGraphRepo()
	svc := service.NewFollowService(graph)

	// Unfollow without ever having followed -- follower count must clamp at 0.
	if err := svc.Unfollow(context.Background(), 2, 1); err != nil {
		t.Fatal(err)
	}
	profile, _ := graph.GetProfile(context.Background(), 1)
	if profile.FollowerCount != 0 {
		t.Errorf("follower count went negative: %d", profile.FollowerCount)
	}
}

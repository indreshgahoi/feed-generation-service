package service_test

import (
	"context"
	"errors"
	"testing"

	"post-ingestion-service/internal/domain"
	"post-ingestion-service/internal/service"
)

func TestUserService_CreateUser_WritesToAllThreeStores(t *testing.T) {
	users := newMockUserRepo()
	graph := newMockGraphRepo()
	directory := newMockUsernameDirectory()
	minter := newMockIDMinter(100)
	svc := service.NewUserService(users, graph, directory, minter)

	user, err := svc.CreateUser(context.Background(), "alice")
	if err != nil {
		t.Fatalf("CreateUser returned error: %v", err)
	}
	if user.Username != "alice" {
		t.Errorf("got username %q, want alice", user.Username)
	}
	if _, ok := users.users[user.UserID]; !ok {
		t.Error("user was not written to the user repository")
	}
	if len(graph.ensureCalls) != 1 || graph.ensureCalls[0] != user.UserID {
		t.Errorf("EnsureUserNode not called with the new user's ID: %v", graph.ensureCalls)
	}
	if id, ok := directory.entries["alice"]; !ok || id != user.UserID {
		t.Errorf("username directory not populated: %v", directory.entries)
	}
	if len(minter.newEntityCalls) != 1 || minter.newEntityCalls[0] != "alice" {
		t.Errorf("expected a NEW-entity placement decision keyed by username, got %v", minter.newEntityCalls)
	}
}

func TestUserService_CreateUser_RejectsEmptyUsername(t *testing.T) {
	svc := service.NewUserService(newMockUserRepo(), newMockGraphRepo(), newMockUsernameDirectory(), newMockIDMinter(0))
	_, err := svc.CreateUser(context.Background(), "")
	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput for empty username, got %v", err)
	}
}

func TestUserService_ListUsers_DegradesGracefullyWithoutGraphProfile(t *testing.T) {
	users := newMockUserRepo()
	users.users[1] = domain.User{UserID: 1, Username: "orphan"}
	graph := newMockGraphRepo() // no profile for user 1 -- simulates the Postgres/Neo4j sync gap

	svc := service.NewUserService(users, graph, newMockUsernameDirectory(), newMockIDMinter(0))
	result, err := svc.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("ListUsers returned error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 user, got %d", len(result))
	}
	if result[0].FollowerCount != 0 || result[0].IsCelebrity {
		t.Errorf("expected zero-value social profile for a user missing from the graph, got %+v", result[0])
	}
}

func TestUserService_ListUsers_HydratesSocialProfile(t *testing.T) {
	users := newMockUserRepo()
	users.users[1] = domain.User{UserID: 1, Username: "star"}
	graph := newMockGraphRepo()
	graph.profiles[1] = domain.SocialProfile{UserID: 1, Username: "star", FollowerCount: 30000, IsCelebrity: true}

	svc := service.NewUserService(users, graph, newMockUsernameDirectory(), newMockIDMinter(0))
	result, err := svc.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("ListUsers returned error: %v", err)
	}
	if len(result) != 1 || !result[0].IsCelebrity || result[0].FollowerCount != 30000 {
		t.Errorf("expected hydrated celebrity profile, got %+v", result)
	}
}

// Package service holds business use-cases. It depends only on
// domain interfaces -- never on pgx, the Neo4j driver, or Redis directly
// -- which is what lets *_test.go mock every dependency and test business
// logic without standing up any infrastructure.
package service

import (
	"context"
	"fmt"
	"time"

	"post-ingestion-service/internal/domain"
)

type UserService struct {
	users     domain.UserRepository
	graph     domain.SocialGraphRepository
	usernames domain.UsernameDirectory
	minter    domain.IDMinter
	clock     func() time.Time
}

func NewUserService(
	users domain.UserRepository,
	graph domain.SocialGraphRepository,
	usernames domain.UsernameDirectory,
	minter domain.IDMinter,
) *UserService {
	return &UserService{users: users, graph: graph, usernames: usernames, minter: minter, clock: time.Now}
}

// CreateUser places the user on a shard chosen by the consistent-hash
// ring (this is the ONE placement decision the ring makes; every other
// entity inherits a shard instead -- see doc/DESIGN.md), then writes
// the identity row to sharded Postgres, a matching graph node to Neo4j,
// and a username->id entry to the Redis directory. The Neo4j and
// directory writes are best-effort follow-ups to the Postgres write of
// record; see doc/DESIGN.md "What still has to stay in sync" for the
// consistency gap this creates.
func (s *UserService) CreateUser(ctx context.Context, username string) (domain.User, error) {
	if username == "" {
		return domain.User{}, fmt.Errorf("%w: username is required", domain.ErrInvalidInput)
	}

	userID := s.minter.NewIDForNewEntity(username)
	user := domain.User{UserID: userID, Username: username, LastActiveAt: s.clock()}

	if err := s.users.Create(ctx, user); err != nil {
		return domain.User{}, fmt.Errorf("create user: %w", err)
	}
	if err := s.graph.EnsureUserNode(ctx, userID, username); err != nil {
		return domain.User{}, fmt.Errorf("create user: sync to graph: %w", err)
	}
	if err := s.usernames.Set(ctx, username, userID); err != nil {
		return domain.User{}, fmt.Errorf("create user: sync to directory: %w", err)
	}
	return user, nil
}

func (s *UserService) GetUser(ctx context.Context, userID int64) (domain.User, error) {
	return s.users.GetByID(ctx, userID)
}

// UserWithProfile joins the sharded identity row with Neo4j-derived
// social facts -- two different stores, joined in the service layer
// because no single query can span them.
type UserWithProfile struct {
	domain.User
	FollowerCount int
	IsCelebrity   bool
}

// ListUsers fans out across all shards (see UserRepository.ListAll) and
// hydrates each with its social profile. Demo/admin-scale only -- see
// doc/DESIGN.md.
func (s *UserService) ListUsers(ctx context.Context) ([]UserWithProfile, error) {
	users, err := s.users.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}

	result := make([]UserWithProfile, 0, len(users))
	for _, u := range users {
		profile, err := s.graph.GetProfile(ctx, u.UserID)
		if err != nil {
			// A user existing in Postgres but not yet in Neo4j is exactly
			// the sync gap documented in doc/DESIGN.md -- degrade
			// gracefully rather than failing the whole list.
			result = append(result, UserWithProfile{User: u})
			continue
		}
		result = append(result, UserWithProfile{User: u, FollowerCount: profile.FollowerCount, IsCelebrity: profile.IsCelebrity})
	}
	return result, nil
}

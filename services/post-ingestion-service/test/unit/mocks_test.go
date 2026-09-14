package service_test

import (
	"context"

	"post-ingestion-service/internal/domain"
)

// Hand-rolled mocks, not a mocking framework -- these interfaces are
// small enough that a generated mock would add a dependency for no real
// benefit. Each mock records calls it needs to assert on and lets a test
// inject failures via a *Err field.

type mockUserRepo struct {
	users      map[int64]domain.User
	createErr  error
	listErr    error
	createCall *domain.User
}

func newMockUserRepo() *mockUserRepo { return &mockUserRepo{users: map[int64]domain.User{}} }

func (m *mockUserRepo) Create(_ context.Context, u domain.User) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.createCall = &u
	m.users[u.UserID] = u
	return nil
}

func (m *mockUserRepo) GetByID(_ context.Context, id int64) (domain.User, error) {
	u, ok := m.users[id]
	if !ok {
		return domain.User{}, domain.ErrNotFound
	}
	return u, nil
}

func (m *mockUserRepo) ListAll(_ context.Context) ([]domain.User, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	var out []domain.User
	for _, u := range m.users {
		out = append(out, u)
	}
	return out, nil
}

type mockGraphRepo struct {
	profiles    map[int64]domain.SocialProfile
	following   map[int64][]int64
	ensureErr   error
	followErr   error
	unfollowErr error
	ensureCalls []int64
	followCalls [][2]int64
}

func newMockGraphRepo() *mockGraphRepo {
	return &mockGraphRepo{profiles: map[int64]domain.SocialProfile{}, following: map[int64][]int64{}}
}

func (m *mockGraphRepo) EnsureUserNode(_ context.Context, userID int64, username string) error {
	if m.ensureErr != nil {
		return m.ensureErr
	}
	m.ensureCalls = append(m.ensureCalls, userID)
	m.profiles[userID] = domain.SocialProfile{UserID: userID, Username: username}
	return nil
}

func (m *mockGraphRepo) Follow(_ context.Context, followerID, followeeID int64) error {
	if followerID == followeeID {
		return domain.ErrSelfFollow
	}
	if m.followErr != nil {
		return m.followErr
	}
	m.followCalls = append(m.followCalls, [2]int64{followerID, followeeID})
	m.following[followerID] = append(m.following[followerID], followeeID)
	profile := m.profiles[followeeID]
	profile.FollowerCount++
	profile.IsCelebrity = profile.FollowerCount > 25000
	m.profiles[followeeID] = profile
	return nil
}

func (m *mockGraphRepo) Unfollow(_ context.Context, followerID, followeeID int64) error {
	if m.unfollowErr != nil {
		return m.unfollowErr
	}
	ids := m.following[followerID]
	for i, id := range ids {
		if id == followeeID {
			m.following[followerID] = append(ids[:i], ids[i+1:]...)
			break
		}
	}
	profile := m.profiles[followeeID]
	if profile.FollowerCount > 0 {
		profile.FollowerCount--
	}
	profile.IsCelebrity = profile.FollowerCount > 25000
	m.profiles[followeeID] = profile
	return nil
}

func (m *mockGraphRepo) Following(_ context.Context, userID int64) ([]int64, error) {
	return m.following[userID], nil
}

func (m *mockGraphRepo) GetProfile(_ context.Context, userID int64) (domain.SocialProfile, error) {
	p, ok := m.profiles[userID]
	if !ok {
		return domain.SocialProfile{}, domain.ErrNotFound
	}
	return p, nil
}

type mockUsernameDirectory struct {
	entries map[string]int64
	setErr  error
}

func newMockUsernameDirectory() *mockUsernameDirectory {
	return &mockUsernameDirectory{entries: map[string]int64{}}
}

func (m *mockUsernameDirectory) Set(_ context.Context, username string, userID int64) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.entries[username] = userID
	return nil
}

func (m *mockUsernameDirectory) Lookup(_ context.Context, username string) (int64, bool, error) {
	id, ok := m.entries[username]
	return id, ok, nil
}

type mockIDMinter struct {
	nextID         int64
	inheritCalls   []int64
	newEntityCalls []string
}

func newMockIDMinter(start int64) *mockIDMinter { return &mockIDMinter{nextID: start} }

func (m *mockIDMinter) NewIDInheritingShard(existingID int64) int64 {
	m.inheritCalls = append(m.inheritCalls, existingID)
	m.nextID++
	return m.nextID
}

func (m *mockIDMinter) NewIDForNewEntity(placementKey string) int64 {
	m.newEntityCalls = append(m.newEntityCalls, placementKey)
	m.nextID++
	return m.nextID
}

type mockPostRepo struct {
	posts     []domain.Post
	createErr error
}

func (m *mockPostRepo) Create(_ context.Context, p domain.Post) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.posts = append(m.posts, p)
	return nil
}

func (m *mockPostRepo) ListRecentByAuthors(_ context.Context, userIDs []int64, limitPerAuthor int) ([]domain.Post, error) {
	var out []domain.Post
	for _, p := range m.posts {
		for _, id := range userIDs {
			if p.UserID == id {
				out = append(out, p)
			}
		}
	}
	return out, nil
}

type mockPublisher struct {
	published  []domain.Post
	publishErr error
}

func (m *mockPublisher) PublishPostCreated(_ context.Context, p domain.Post) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.published = append(m.published, p)
	return nil
}

type mockLikeRepo struct {
	likes map[[2]int64]bool // [postID, userID] -> exists
}

func newMockLikeRepo() *mockLikeRepo { return &mockLikeRepo{likes: map[[2]int64]bool{}} }

func (m *mockLikeRepo) Create(_ context.Context, like domain.Like) (bool, error) {
	key := [2]int64{like.PostID, like.UserID}
	if m.likes[key] {
		return false, nil
	}
	m.likes[key] = true
	return true, nil
}

func (m *mockLikeRepo) Delete(_ context.Context, postID, userID int64) (bool, error) {
	key := [2]int64{postID, userID}
	if !m.likes[key] {
		return false, nil
	}
	delete(m.likes, key)
	return true, nil
}

type mockCommentRepo struct {
	byPost map[int64][]domain.Comment
}

func newMockCommentRepo() *mockCommentRepo {
	return &mockCommentRepo{byPost: map[int64][]domain.Comment{}}
}

func (m *mockCommentRepo) Create(_ context.Context, c domain.Comment) error {
	m.byPost[c.PostID] = append(m.byPost[c.PostID], c)
	return nil
}

func (m *mockCommentRepo) ListByPost(_ context.Context, postID int64) ([]domain.Comment, error) {
	return m.byPost[postID], nil
}

type mockCounterRepo struct {
	likeCounts    map[int64]int64
	commentCounts map[int64]int64
}

func newMockCounterRepo() *mockCounterRepo {
	return &mockCounterRepo{likeCounts: map[int64]int64{}, commentCounts: map[int64]int64{}}
}

func (m *mockCounterRepo) IncrLikeCount(_ context.Context, postID, _ int64) (int64, error) {
	m.likeCounts[postID]++
	return m.likeCounts[postID], nil
}

func (m *mockCounterRepo) DecrLikeCount(_ context.Context, postID, _ int64) (int64, error) {
	if m.likeCounts[postID] > 0 {
		m.likeCounts[postID]--
	}
	return m.likeCounts[postID], nil
}

func (m *mockCounterRepo) IncrCommentCount(_ context.Context, postID int64) (int64, error) {
	m.commentCounts[postID]++
	return m.commentCounts[postID], nil
}

func (m *mockCounterRepo) GetLikeCount(_ context.Context, postID int64) (int64, error) {
	return m.likeCounts[postID], nil
}

type mockLikeState struct {
	liked map[[2]int64]bool // [userID, postID] -> liked
}

func newMockLikeState() *mockLikeState { return &mockLikeState{liked: map[[2]int64]bool{}} }

func (m *mockLikeState) MarkLiked(_ context.Context, userID, postID int64) error {
	m.liked[[2]int64{userID, postID}] = true
	return nil
}

func (m *mockLikeState) MarkUnliked(_ context.Context, userID, postID int64) error {
	delete(m.liked, [2]int64{userID, postID})
	return nil
}

type mockRateLimiter struct {
	allow bool // defaults to false (zero value); tests opt in via newMockRateLimiter
}

func newMockRateLimiter() *mockRateLimiter { return &mockRateLimiter{allow: true} }

func (m *mockRateLimiter) Allow(_ context.Context, _ string) (bool, error) {
	return m.allow, nil
}

type mockModerator struct {
	allow bool
}

func newMockModerator() *mockModerator { return &mockModerator{allow: true} }

func (m *mockModerator) IsAllowed(_ string) bool {
	return m.allow
}

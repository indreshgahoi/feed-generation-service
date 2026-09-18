// Package domain holds entities and repository interfaces only -- no SQL,
// no Cypher, no Redis commands, no HTTP. The service layer depends on
// these interfaces; the storage layer implements them. This is what lets
// service_test.go mock a repository instead of standing up Postgres/Neo4j
// to test business logic.
package domain

import "time"

type MediaType int16

const (
	MediaTypeImage    MediaType = 1
	MediaTypeVideo    MediaType = 2
	MediaTypeCarousel MediaType = 3
)

func (m MediaType) Valid() bool {
	return m >= MediaTypeImage && m <= MediaTypeCarousel
}

// User is an identity/profile record -- lives in sharded Postgres.
// Social facts (follower count, celebrity status, the follow graph
// itself) live on the Neo4j SocialProfile instead; see doc/DESIGN.md.
type User struct {
	UserID       int64
	Username     string
	LastActiveAt time.Time
}

// Post is co-located with its author's shard: PostID embeds the same
// shard bits as UserID (inherited at creation, not re-hashed).
type Post struct {
	PostID    int64
	UserID    int64
	MediaURL  string
	MediaType MediaType
	Caption   string
	CreatedAt time.Time
}

// Like is co-located with the liking user's shard, not the post's --
// see doc/DESIGN.md.
type Like struct {
	PostID    int64
	UserID    int64
	CreatedAt time.Time
}

// Comment is co-located with the post's shard, not the commenter's.
type Comment struct {
	CommentID int64
	PostID    int64
	UserID    int64
	Username  string // denormalized at read time for API responses
	Body      string
	CreatedAt time.Time
}

// SocialProfile is the Neo4j-backed view of a user's place in the social
// graph -- see doc/DESIGN.md "The social graph lives in Neo4j, not
// sharded Postgres".
type SocialProfile struct {
	UserID        int64
	Username      string
	FollowerCount int
	IsCelebrity   bool
}

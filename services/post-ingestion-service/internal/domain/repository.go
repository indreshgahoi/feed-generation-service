package domain

import "context"

// UserRepository is backed by sharded Postgres.
type UserRepository interface {
	Create(ctx context.Context, user User) error
	GetByID(ctx context.Context, userID int64) (User, error)
	// ListAll fans out to every shard -- see doc/DESIGN.md, "List all
	// users": inherently global, admin/demo-scale only.
	ListAll(ctx context.Context) ([]User, error)
}

// PostRepository is backed by sharded Postgres, co-located with the
// author's shard.
type PostRepository interface {
	Create(ctx context.Context, post Post) error
	// ListRecentByAuthors groups userIDs by shard, fans out in parallel,
	// and merges by CreatedAt -- the one cross-shard scatter-gather a
	// graph database doesn't remove, because it's about post storage, not
	// the social graph. See doc/DESIGN.md.
	ListRecentByAuthors(ctx context.Context, userIDs []int64, limitPerAuthor int) ([]Post, error)
}

// LikeRepository is backed by sharded Postgres, co-located with the
// LIKING user's shard (not the post's) -- see doc/DESIGN.md.
type LikeRepository interface {
	// Create is idempotent: returns created=false if the row already
	// existed rather than erroring.
	Create(ctx context.Context, like Like) (created bool, err error)
	Delete(ctx context.Context, postID, userID int64) (deleted bool, err error)
}

// CommentRepository is backed by sharded Postgres, co-located with the
// POST's shard (not the commenter's) -- see doc/DESIGN.md.
type CommentRepository interface {
	Create(ctx context.Context, comment Comment) error
	ListByPost(ctx context.Context, postID int64) ([]Comment, error)
}

// SocialGraphRepository is backed by Neo4j, not sharded Postgres -- see
// doc/DESIGN.md "The social graph lives in Neo4j, not sharded Postgres".
type SocialGraphRepository interface {
	EnsureUserNode(ctx context.Context, userID int64, username string) error
	Follow(ctx context.Context, followerID, followeeID int64) error
	Unfollow(ctx context.Context, followerID, followeeID int64) error
	Following(ctx context.Context, userID int64) ([]int64, error)
	GetProfile(ctx context.Context, userID int64) (SocialProfile, error)
}

// CounterRepository is backed by Redis -- see doc/DESIGN.md "Counters
// live in Redis, not the sharded database" and doc/DESIGN.md
// for why like counters are further split across N sub-shards keyed by
// the liking user, not a single per-post key.
type CounterRepository interface {
	IncrLikeCount(ctx context.Context, postID, userID int64) (int64, error)
	DecrLikeCount(ctx context.Context, postID, userID int64) (int64, error)
	IncrCommentCount(ctx context.Context, postID int64) (int64, error)
	GetLikeCount(ctx context.Context, postID int64) (int64, error)
}

// UsernameDirectory is backed by Redis -- see doc/DESIGN.md "The
// username problem (a global secondary index)".
type UsernameDirectory interface {
	Set(ctx context.Context, username string, userID int64) error
	Lookup(ctx context.Context, username string) (userID int64, found bool, err error)
}

// LikeStateRepository is the Redis-backed read-your-own-writes cache:
// "does THIS viewer currently like THIS post," answerable in one Redis
// round trip regardless of which Postgres shard either of them lives on.
// See doc/DESIGN.md. The sharded Postgres `likes` table
// (LikeRepository) remains the durable system of record; this is a fast
// projection of it.
type LikeStateRepository interface {
	MarkLiked(ctx context.Context, userID, postID int64) error
	MarkUnliked(ctx context.Context, userID, postID int64) error
}

// RateLimiter guards against like/unlike "flapping" (rapid toggling, by
// bots or impatient double-taps) -- see doc/DESIGN.md "Like
// / Unlike Spam".
type RateLimiter interface {
	// Allow reports whether the action identified by key may proceed,
	// consuming one unit of the caller's budget if so.
	Allow(ctx context.Context, key string) (bool, error)
}

// ContentModerator is a synchronous pre-filter checked before a comment
// is persisted -- see doc/DESIGN.md "Toxicity / Spam
// Injection". Real ML-based async moderation is explicitly out of scope;
// see that doc for why.
type ContentModerator interface {
	IsAllowed(text string) bool
}

// EventPublisher decouples post creation from fan-out/vector/notification
// processing via Kafka.
type EventPublisher interface {
	PublishPostCreated(ctx context.Context, post Post) error
}

// IDMinter is the service layer's only view of sharding: it can mint an
// ID that inherits an existing entity's shard (e.g. a post inheriting
// its author's shard), or place a brand-new entity via the consistent-
// hash ring. The service layer never computes a shard ID itself -- see
// doc/DESIGN.md.
type IDMinter interface {
	NewIDInheritingShard(existingID int64) int64
	NewIDForNewEntity(placementKey string) int64
}

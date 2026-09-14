package domain

import "context"

// HotInboxRepository is the Redis-backed hot tier: `feed:user:<id>` /
// `celebrity:outbox:<id>` ZSETs for users active within
// ACTIVE_WITHIN_DAYS. See doc/caching.md.
type HotInboxRepository interface {
	GetInbox(ctx context.Context, userID string, limit int) ([]Candidate, error)
	GetCelebrityOutbox(ctx context.Context, celebrityID string, limit int) ([]Candidate, error)
	// Exists reports whether userID has a hot-tier inbox at all. false
	// is the signal that this user has been dormant and their data (if
	// any) is in the cold tier instead -- see doc/caching.md.
	Exists(ctx context.Context, userID string) (bool, error)
	// Promote copies cold-tier candidates into a fresh hot-tier inbox --
	// called when a dormant user reads their feed, since a read means
	// they're active again now.
	Promote(ctx context.Context, userID string, candidates []Candidate) error
}

// ColdInboxRepository is the BadgerDB-backed cold tier for dormant users
// -- see doc/caching.md for the RocksDB->BadgerDB substitution rationale.
type ColdInboxRepository interface {
	Get(ctx context.Context, userID string) ([]Candidate, error)
	// Append is called by fanout-worker (via the internal HTTP endpoint)
	// when fanning out a post to a DORMANT follower, instead of the
	// pre-tiering behavior of simply dropping that fan-out write.
	Append(ctx context.Context, userID string, candidate Candidate) error
}

// SeenStateRepository is the Redis ZSET seen-state dedupe -- see
// doc/flow.md and doc/trade-offs.md for why a bounded-TTL ZSET
// approximates a Bloom filter here.
type SeenStateRepository interface {
	FilterUnseen(ctx context.Context, userID string, postIDs []string, ttlSeconds int) ([]string, error)
	MarkSeen(ctx context.Context, userID string, postIDs []string) error
	// Reset is a LOCAL DEMO CONVENIENCE ONLY -- see doc/README, "Reset
	// seen (demo)". A real feed never needs this.
	Reset(ctx context.Context, userID string) error
}

// SocialGraphRepository is the Neo4j-backed, read-only (from this
// service's perspective) view of the social graph. See doc/sharding.md
// "The social graph lives in Neo4j, not sharded Postgres" -- this single
// Cypher query replaces what used to be a cross-shard scatter-gather to
// check each followee's celebrity flag.
type SocialGraphRepository interface {
	FollowedCelebrities(ctx context.Context, userID string) ([]string, error)
	Following(ctx context.Context, userID string) ([]string, error)
}

// PostMetaRepository is the sharded-Postgres-backed source of post
// content (media/caption/author/created_at) -- NOT engagement counts,
// which live in Redis (CounterRepository) per doc/sharding.md "Counters
// live in Redis, not the sharded database".
type PostMetaRepository interface {
	GetBatch(ctx context.Context, postIDs []string) (map[string]PostMeta, error)
	// RecentByAuthors groups authorIDs by shard and fans out in parallel
	// -- the one cross-shard scatter-gather a graph database doesn't
	// remove. See doc/sharding.md.
	RecentByAuthors(ctx context.Context, authorIDs []string, limitPerAuthor int) ([]PostMeta, error)
}

// CounterRepository reads the Redis-backed engagement counters written
// by post-ingestion-service. See doc/sharding.md.
type CounterRepository interface {
	BatchGetCounts(ctx context.Context, postIDs []string) (likeCounts, commentCounts map[string]int64, err error)
}

// LikedRepository checks which of a batch of candidate posts the VIEWER
// has liked -- a single-shard query against sharded Postgres, since
// `likes` rows are co-located with the LIKING user (the viewer here),
// not the post. See doc/sharding.md.
type LikedRepository interface {
	BatchIsLiked(ctx context.Context, viewerID string, postIDs []string) (map[string]bool, error)
}

// VectorRepository wraps Qdrant ANN search for out-of-network recall.
type VectorRepository interface {
	// DeriveTasteVector averages the embeddings of a set of seed posts --
	// a stand-in for a learned two-tower user embedding. Returns ok=false
	// if none of the seed posts have embeddings yet.
	DeriveTasteVector(ctx context.Context, seedPostIDs []string) (vector []float64, ok bool)
	Search(ctx context.Context, vector []float64, limit int) ([]Candidate, error)
}

// RankingClient calls the (Rust) ranking-service over HTTP.
type RankingClient interface {
	Rank(ctx context.Context, metas []PostMeta, sourceByPostID map[string]string) ([]RankedItem, error)
}

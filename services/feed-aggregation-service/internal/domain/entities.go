// Package domain holds entities and repository interfaces only. The
// service layer (internal/service) depends on these interfaces; every
// concrete store (sharded Postgres, Neo4j, Redis, BadgerDB, Qdrant,
// ranking-service) implements one in internal/storage. See
// post-ingestion-service's domain package for the same pattern, and
// doc/architecture.md for why both Go services share it.
package domain

import "time"

// Candidate is a raw feed candidate before hydration/ranking -- just
// enough to know where it came from and merge/dedupe it.
type Candidate struct {
	PostID   string
	AuthorID string
	Source   string // in-network | celebrity | vector
}

// PostMeta is a candidate hydrated with the metadata needed to rank and
// render it.
type PostMeta struct {
	PostID       string
	AuthorID     string
	MediaURL     string
	Caption      string
	LikeCount    int64
	CommentCount int64
	CreatedAt    time.Time
}

// RankedItem is what ranking-service returns: a candidate with a
// composite score attached.
type RankedItem struct {
	PostID   string
	AuthorID string
	Source   string
	Score    float64
}

// FeedItem is what the client actually receives.
type FeedItem struct {
	PostID       string
	AuthorID     string
	MediaURL     string
	Caption      string
	LikeCount    int64
	CommentCount int64
	Source       string
	Score        float64
	IsAd         bool
	CreatedAt    time.Time
	LikedByMe    bool
}

type FeedCursor struct {
	Offset int
}

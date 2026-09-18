// Package service holds the read-path orchestration. It depends only on
// domain interfaces -- see doc/DESIGN.md for the stage-by-stage narrative
// this code implements, and doc/DESIGN.md / doc/DESIGN.md for why
// each store is involved the way it is.
package service

import (
	"context"
	"log/slog"
	"sync"

	"feed-aggregation-service/internal/domain"
)

type FeedConfig struct {
	InNetworkCandidateLimit int
	CelebrityCandidateLimit int
	VectorCandidateLimit    int
	SeenStateTTLSeconds     int
	PageSize                int
	MaxPostsPerAuthor       int
	TasteSeedPostLimit      int
}

type FeedService struct {
	hotInbox  domain.HotInboxRepository
	coldInbox domain.ColdInboxRepository
	seenState domain.SeenStateRepository
	graph     domain.SocialGraphRepository
	postMeta  domain.PostMetaRepository
	counters  domain.CounterRepository
	liked     domain.LikedRepository
	vector    domain.VectorRepository
	ranking   domain.RankingClient
	cursor    *CursorCodec
	cfg       FeedConfig
}

func NewFeedService(
	hotInbox domain.HotInboxRepository,
	coldInbox domain.ColdInboxRepository,
	seenState domain.SeenStateRepository,
	graph domain.SocialGraphRepository,
	postMeta domain.PostMetaRepository,
	counters domain.CounterRepository,
	liked domain.LikedRepository,
	vector domain.VectorRepository,
	ranking domain.RankingClient,
	cursor *CursorCodec,
	cfg FeedConfig,
) *FeedService {
	return &FeedService{
		hotInbox: hotInbox, coldInbox: coldInbox, seenState: seenState, graph: graph,
		postMeta: postMeta, counters: counters, liked: liked, vector: vector,
		ranking: ranking, cursor: cursor, cfg: cfg,
	}
}

type FeedResult struct {
	Items      []domain.FeedItem
	NextCursor string
}

func (s *FeedService) GetFeed(ctx context.Context, userID string, cursorToken string) (FeedResult, error) {
	cursor, err := s.cursor.Decode(cursorToken)
	if err != nil {
		return FeedResult{}, err
	}

	inNetwork, celebrity, vectorCands := s.fanIn(ctx, userID)

	// Stage 2: merge + dedupe (~800 raw candidates in the doc).
	sourceByPostID := make(map[string]string)
	ordered := make([]string, 0, len(inNetwork)+len(celebrity)+len(vectorCands))
	for _, group := range [][]domain.Candidate{inNetwork, celebrity, vectorCands} {
		for _, c := range group {
			if _, seen := sourceByPostID[c.PostID]; seen {
				continue
			}
			sourceByPostID[c.PostID] = c.Source
			ordered = append(ordered, c.PostID)
		}
	}

	// Stage 3: seen-state filter.
	unseen, err := s.seenState.FilterUnseen(ctx, userID, ordered, s.cfg.SeenStateTTLSeconds)
	if err != nil {
		slog.Warn("seen-state filter failed, proceeding unfiltered", "userID", userID, "error", err)
		unseen = ordered
	}

	// Stage 4: hydrate content + engagement counts (two different
	// stores -- see doc/DESIGN.md "Counters live in Redis").
	metaByID, err := s.postMeta.GetBatch(ctx, unseen)
	if err != nil {
		return FeedResult{}, err
	}
	likeCounts, commentCounts, err := s.counters.BatchGetCounts(ctx, unseen)
	if err != nil {
		slog.Warn("counter batch fetch failed, defaulting to zero", "error", err)
		likeCounts, commentCounts = map[string]int64{}, map[string]int64{}
	}
	for id, meta := range metaByID {
		meta.LikeCount = likeCounts[id]
		meta.CommentCount = commentCounts[id]
		metaByID[id] = meta
	}
	metas := make([]domain.PostMeta, 0, len(metaByID))
	for _, m := range metaByID {
		metas = append(metas, m)
	}

	// Stage 5: ranking-service scoring pass.
	var ranked []domain.RankedItem
	if len(metas) > 0 {
		ranked, err = s.ranking.Rank(ctx, metas, sourceByPostID)
		if err != nil {
			return FeedResult{}, err
		}
	}

	// Stage 6: diversity rule, then cursor-windowed pagination.
	diversified := applyDiversity(ranked, s.cfg.MaxPostsPerAuthor)
	start := cursor.Offset
	if start > len(diversified) {
		start = len(diversified)
	}
	end := start + s.cfg.PageSize - 2 // reserve 2 organic slots for ad insertion
	if end > len(diversified) {
		end = len(diversified)
	}
	page := diversified[start:end]

	likedByMe, err := s.liked.BatchIsLiked(ctx, userID, postIDsOf(page))
	if err != nil {
		slog.Warn("liked-batch lookup failed, defaulting to false", "userID", userID, "error", err)
		likedByMe = map[string]bool{}
	}

	items := make([]domain.FeedItem, 0, len(page))
	returnedPostIDs := make([]string, 0, len(page))
	for _, r := range page {
		m := metaByID[r.PostID]
		items = append(items, domain.FeedItem{
			PostID: r.PostID, AuthorID: r.AuthorID, MediaURL: m.MediaURL, Caption: m.Caption,
			LikeCount: m.LikeCount, CommentCount: m.CommentCount, Source: r.Source, Score: r.Score,
			CreatedAt: m.CreatedAt, LikedByMe: likedByMe[r.PostID],
		})
		returnedPostIDs = append(returnedPostIDs, r.PostID)
	}

	// Stage 7: ad insertion.
	finalItems := insertAds(items, s.cfg.PageSize)

	// Stage 8: mark seen.
	if err := s.seenState.MarkSeen(ctx, userID, returnedPostIDs); err != nil {
		slog.Warn("failed to mark posts seen", "userID", userID, "error", err)
	}

	nextCursor, err := s.cursor.Encode(domain.FeedCursor{Offset: end})
	if err != nil {
		slog.Warn("failed to encode next cursor", "error", err)
	}

	return FeedResult{Items: finalItems, NextCursor: nextCursor}, nil
}

// fanIn retrieves the three candidate sources concurrently -- see
// doc/DESIGN.md Stage 1. The in-network source additionally implements
// hot/cold promotion: see doc/DESIGN.md.
func (s *FeedService) fanIn(ctx context.Context, userID string) (inNetwork, celebrity, vector []domain.Candidate) {
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		inNetwork = s.inNetworkCandidates(ctx, userID)
	}()

	go func() {
		defer wg.Done()
		celebrity = s.celebrityCandidates(ctx, userID)
	}()

	go func() {
		defer wg.Done()
		vector = s.vectorCandidates(ctx, userID)
	}()

	wg.Wait()
	return inNetwork, celebrity, vector
}

func (s *FeedService) inNetworkCandidates(ctx context.Context, userID string) []domain.Candidate {
	exists, err := s.hotInbox.Exists(ctx, userID)
	if err != nil {
		slog.Warn("hot inbox existence check failed", "userID", userID, "error", err)
		return nil
	}

	if exists {
		cands, err := s.hotInbox.GetInbox(ctx, userID, s.cfg.InNetworkCandidateLimit)
		if err != nil {
			slog.Warn("hot inbox fetch failed", "userID", userID, "error", err)
			return nil
		}
		return cands
	}

	// Hot tier miss: this user has been dormant. Fall back to the cold
	// tier and, if we find anything, promote it back to the hot tier --
	// a read means they're active again now. See doc/DESIGN.md.
	cold, err := s.coldInbox.Get(ctx, userID)
	if err != nil || len(cold) == 0 {
		return nil
	}
	if err := s.hotInbox.Promote(ctx, userID, cold); err != nil {
		slog.Warn("failed to promote cold-tier inbox to hot tier", "userID", userID, "error", err)
	}
	if len(cold) > s.cfg.InNetworkCandidateLimit {
		cold = cold[:s.cfg.InNetworkCandidateLimit]
	}
	return cold
}

func (s *FeedService) celebrityCandidates(ctx context.Context, userID string) []domain.Candidate {
	celebIDs, err := s.graph.FollowedCelebrities(ctx, userID)
	if err != nil || len(celebIDs) == 0 {
		if err != nil {
			slog.Warn("followed-celebrities lookup failed", "userID", userID, "error", err)
		}
		return nil
	}

	perCeleb := s.cfg.CelebrityCandidateLimit / len(celebIDs)
	if perCeleb < 1 {
		perCeleb = 1
	}

	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		cands []domain.Candidate
	)
	for _, celebID := range celebIDs {
		wg.Add(1)
		go func(celebID string) {
			defer wg.Done()
			outbox, err := s.hotInbox.GetCelebrityOutbox(ctx, celebID, perCeleb)
			if err != nil {
				slog.Warn("celebrity outbox fetch failed", "celebrityID", celebID, "error", err)
				return
			}
			mu.Lock()
			cands = append(cands, outbox...)
			mu.Unlock()
		}(celebID)
	}
	wg.Wait()
	return cands
}

func (s *FeedService) vectorCandidates(ctx context.Context, userID string) []domain.Candidate {
	followees, err := s.graph.Following(ctx, userID)
	if err != nil {
		slog.Warn("following lookup for taste vector failed", "userID", userID, "error", err)
		return nil
	}
	seedAuthors := append([]string{userID}, followees...)

	seedPosts, err := s.postMeta.RecentByAuthors(ctx, seedAuthors, s.cfg.TasteSeedPostLimit)
	if err != nil || len(seedPosts) == 0 {
		return nil
	}
	seedIDs := make([]string, 0, len(seedPosts))
	for _, p := range seedPosts {
		seedIDs = append(seedIDs, p.PostID)
	}

	tasteVector, ok := s.vector.DeriveTasteVector(ctx, seedIDs)
	if !ok {
		return nil
	}
	cands, err := s.vector.Search(ctx, tasteVector, s.cfg.VectorCandidateLimit)
	if err != nil {
		slog.Warn("vector search failed", "userID", userID, "error", err)
		return nil
	}
	return cands
}

func (s *FeedService) ResetSeenState(ctx context.Context, userID string) error {
	return s.seenState.Reset(ctx, userID)
}

func postIDsOf(items []domain.RankedItem) []string {
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.PostID
	}
	return ids
}

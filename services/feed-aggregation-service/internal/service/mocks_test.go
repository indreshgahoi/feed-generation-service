package service

import (
	"context"

	"feed-aggregation-service/internal/domain"
)

type mockHotInbox struct {
	inboxes    map[string][]domain.Candidate
	outboxes   map[string][]domain.Candidate
	promoted   map[string][]domain.Candidate
	existsFlag map[string]bool
}

func newMockHotInbox() *mockHotInbox {
	return &mockHotInbox{
		inboxes: map[string][]domain.Candidate{}, outboxes: map[string][]domain.Candidate{},
		promoted: map[string][]domain.Candidate{}, existsFlag: map[string]bool{},
	}
}

func (m *mockHotInbox) GetInbox(_ context.Context, userID string, limit int) ([]domain.Candidate, error) {
	cands := m.inboxes[userID]
	if len(cands) > limit {
		cands = cands[:limit]
	}
	return cands, nil
}

func (m *mockHotInbox) GetCelebrityOutbox(_ context.Context, celebID string, limit int) ([]domain.Candidate, error) {
	cands := m.outboxes[celebID]
	if len(cands) > limit {
		cands = cands[:limit]
	}
	return cands, nil
}

func (m *mockHotInbox) Exists(_ context.Context, userID string) (bool, error) {
	return m.existsFlag[userID], nil
}

func (m *mockHotInbox) Promote(_ context.Context, userID string, candidates []domain.Candidate) error {
	m.promoted[userID] = candidates
	m.existsFlag[userID] = true
	m.inboxes[userID] = candidates
	return nil
}

type mockColdInbox struct {
	byUser map[string][]domain.Candidate
}

func newMockColdInbox() *mockColdInbox {
	return &mockColdInbox{byUser: map[string][]domain.Candidate{}}
}

func (m *mockColdInbox) Get(_ context.Context, userID string) ([]domain.Candidate, error) {
	return m.byUser[userID], nil
}

func (m *mockColdInbox) Append(_ context.Context, userID string, c domain.Candidate) error {
	m.byUser[userID] = append(m.byUser[userID], c)
	return nil
}

type mockSeenState struct {
	seen       map[string]map[string]bool
	resetCalls []string
}

func newMockSeenState() *mockSeenState {
	return &mockSeenState{seen: map[string]map[string]bool{}}
}

func (m *mockSeenState) FilterUnseen(_ context.Context, userID string, postIDs []string, _ int) ([]string, error) {
	seenForUser := m.seen[userID]
	var unseen []string
	for _, id := range postIDs {
		if seenForUser == nil || !seenForUser[id] {
			unseen = append(unseen, id)
		}
	}
	return unseen, nil
}

func (m *mockSeenState) MarkSeen(_ context.Context, userID string, postIDs []string) error {
	if m.seen[userID] == nil {
		m.seen[userID] = map[string]bool{}
	}
	for _, id := range postIDs {
		m.seen[userID][id] = true
	}
	return nil
}

func (m *mockSeenState) Reset(_ context.Context, userID string) error {
	m.resetCalls = append(m.resetCalls, userID)
	delete(m.seen, userID)
	return nil
}

type mockGraph struct {
	celebrities map[string][]string
	following   map[string][]string
}

func newMockGraph() *mockGraph {
	return &mockGraph{celebrities: map[string][]string{}, following: map[string][]string{}}
}

func (m *mockGraph) FollowedCelebrities(_ context.Context, userID string) ([]string, error) {
	return m.celebrities[userID], nil
}

func (m *mockGraph) Following(_ context.Context, userID string) ([]string, error) {
	return m.following[userID], nil
}

type mockPostMeta struct {
	byID map[string]domain.PostMeta
}

func newMockPostMeta() *mockPostMeta { return &mockPostMeta{byID: map[string]domain.PostMeta{}} }

func (m *mockPostMeta) GetBatch(_ context.Context, postIDs []string) (map[string]domain.PostMeta, error) {
	out := make(map[string]domain.PostMeta)
	for _, id := range postIDs {
		if meta, ok := m.byID[id]; ok {
			out[id] = meta
		}
	}
	return out, nil
}

func (m *mockPostMeta) RecentByAuthors(_ context.Context, authorIDs []string, _ int) ([]domain.PostMeta, error) {
	var out []domain.PostMeta
	for _, meta := range m.byID {
		for _, id := range authorIDs {
			if meta.AuthorID == id {
				out = append(out, meta)
			}
		}
	}
	return out, nil
}

type mockCounters struct {
	likeCounts    map[string]int64
	commentCounts map[string]int64
}

func newMockCounters() *mockCounters {
	return &mockCounters{likeCounts: map[string]int64{}, commentCounts: map[string]int64{}}
}

func (m *mockCounters) BatchGetCounts(_ context.Context, postIDs []string) (map[string]int64, map[string]int64, error) {
	likes, comments := map[string]int64{}, map[string]int64{}
	for _, id := range postIDs {
		likes[id] = m.likeCounts[id]
		comments[id] = m.commentCounts[id]
	}
	return likes, comments, nil
}

type mockLiked struct {
	liked map[string]map[string]bool // viewerID -> postID -> liked
}

func newMockLiked() *mockLiked { return &mockLiked{liked: map[string]map[string]bool{}} }

func (m *mockLiked) BatchIsLiked(_ context.Context, viewerID string, postIDs []string) (map[string]bool, error) {
	out := make(map[string]bool)
	for _, id := range postIDs {
		out[id] = m.liked[viewerID][id]
	}
	return out, nil
}

type mockVector struct {
	vector       []float64
	vectorOK     bool
	searchResult []domain.Candidate
}

func (m *mockVector) DeriveTasteVector(_ context.Context, _ []string) ([]float64, bool) {
	return m.vector, m.vectorOK
}

func (m *mockVector) Search(_ context.Context, _ []float64, limit int) ([]domain.Candidate, error) {
	cands := m.searchResult
	if len(cands) > limit {
		cands = cands[:limit]
	}
	return cands, nil
}

type mockRanking struct {
	// scoreByID lets a test control ranking order deterministically;
	// unlisted posts default to score 0.
	scoreByID map[string]float64
}

func newMockRanking() *mockRanking { return &mockRanking{scoreByID: map[string]float64{}} }

func (m *mockRanking) Rank(_ context.Context, metas []domain.PostMeta, sourceByPostID map[string]string) ([]domain.RankedItem, error) {
	items := make([]domain.RankedItem, 0, len(metas))
	for _, meta := range metas {
		items = append(items, domain.RankedItem{
			PostID: meta.PostID, AuthorID: meta.AuthorID, Source: sourceByPostID[meta.PostID],
			Score: m.scoreByID[meta.PostID],
		})
	}
	// Stable-ish sort by score descending for deterministic test assertions.
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if items[j].Score > items[i].Score {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
	return items, nil
}

package service_test

import (
	"context"
	"testing"
	"time"

	"feed-aggregation-service/internal/domain"
	"feed-aggregation-service/internal/service"
)

func defaultConfig() service.FeedConfig {
	return service.FeedConfig{
		InNetworkCandidateLimit: 500, CelebrityCandidateLimit: 50, VectorCandidateLimit: 250,
		SeenStateTTLSeconds: 172800, PageSize: 20, MaxPostsPerAuthor: 2, TasteSeedPostLimit: 5,
	}
}

type testFixture struct {
	hotInbox  *mockHotInbox
	coldInbox *mockColdInbox
	seenState *mockSeenState
	graph     *mockGraph
	postMeta  *mockPostMeta
	counters  *mockCounters
	liked     *mockLiked
	vector    *mockVector
	ranking   *mockRanking
	svc       *service.FeedService
}

func newFixture(cfg service.FeedConfig) *testFixture {
	f := &testFixture{
		hotInbox: newMockHotInbox(), coldInbox: newMockColdInbox(), seenState: newMockSeenState(),
		graph: newMockGraph(), postMeta: newMockPostMeta(), counters: newMockCounters(),
		liked: newMockLiked(), vector: &mockVector{}, ranking: newMockRanking(),
	}
	f.svc = service.NewFeedService(f.hotInbox, f.coldInbox, f.seenState, f.graph, f.postMeta, f.counters, f.liked, f.vector, f.ranking, service.NewCursorCodec("test-secret-32-bytes-long-enough"), cfg)
	return f
}

func TestGetFeed_ReturnsInNetworkCandidates(t *testing.T) {
	f := newFixture(defaultConfig())
	f.hotInbox.existsFlag["viewer"] = true
	f.hotInbox.inboxes["viewer"] = []domain.Candidate{{PostID: "1", AuthorID: "author1", Source: "in-network"}}
	f.postMeta.byID["1"] = domain.PostMeta{PostID: "1", AuthorID: "author1", Caption: "hello", CreatedAt: time.Now()}
	f.ranking.scoreByID["1"] = 1.0

	result, err := f.svc.GetFeed(context.Background(), "viewer", "")
	if err != nil {
		t.Fatalf("GetFeed returned error: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].PostID != "1" {
		t.Errorf("expected the in-network candidate to appear, got %+v", result.Items)
	}
}

func TestGetFeed_ColdTierFallback_PromotesOnRead(t *testing.T) {
	f := newFixture(defaultConfig())
	// Viewer has NO hot-tier inbox (dormant) but has cold-tier history.
	f.hotInbox.existsFlag["dormant-viewer"] = false
	f.coldInbox.byUser["dormant-viewer"] = []domain.Candidate{{PostID: "42", AuthorID: "author1", Source: "in-network"}}
	f.postMeta.byID["42"] = domain.PostMeta{PostID: "42", AuthorID: "author1", CreatedAt: time.Now()}
	f.ranking.scoreByID["42"] = 1.0

	result, err := f.svc.GetFeed(context.Background(), "dormant-viewer", "")
	if err != nil {
		t.Fatalf("GetFeed returned error: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].PostID != "42" {
		t.Fatalf("expected cold-tier candidate to surface on hot-tier miss, got %+v", result.Items)
	}
	if len(f.hotInbox.promoted["dormant-viewer"]) != 1 {
		t.Error("expected cold-tier data to be promoted into the hot tier on read (see doc/caching.md)")
	}
}

func TestGetFeed_SeenStateFiltersOutAlreadyShownPosts(t *testing.T) {
	f := newFixture(defaultConfig())
	f.hotInbox.existsFlag["viewer"] = true
	f.hotInbox.inboxes["viewer"] = []domain.Candidate{{PostID: "1", AuthorID: "a"}, {PostID: "2", AuthorID: "a"}}
	f.postMeta.byID["1"] = domain.PostMeta{PostID: "1", AuthorID: "a", CreatedAt: time.Now()}
	f.postMeta.byID["2"] = domain.PostMeta{PostID: "2", AuthorID: "a", CreatedAt: time.Now()}
	f.seenState.seen["viewer"] = map[string]bool{"1": true} // post 1 already shown

	result, err := f.svc.GetFeed(context.Background(), "viewer", "")
	if err != nil {
		t.Fatalf("GetFeed returned error: %v", err)
	}
	for _, item := range result.Items {
		if item.PostID == "1" {
			t.Error("expected post 1 to be filtered out as already-seen")
		}
	}
}

func TestGetFeed_MarksReturnedPostsAsSeen(t *testing.T) {
	f := newFixture(defaultConfig())
	f.hotInbox.existsFlag["viewer"] = true
	f.hotInbox.inboxes["viewer"] = []domain.Candidate{{PostID: "1", AuthorID: "a"}}
	f.postMeta.byID["1"] = domain.PostMeta{PostID: "1", AuthorID: "a", CreatedAt: time.Now()}

	if _, err := f.svc.GetFeed(context.Background(), "viewer", ""); err != nil {
		t.Fatal(err)
	}
	if !f.seenState.seen["viewer"]["1"] {
		t.Error("expected post 1 to be marked seen after being returned")
	}
}

func TestGetFeed_DiversityCapsPostsPerAuthor(t *testing.T) {
	cfg := defaultConfig()
	cfg.MaxPostsPerAuthor = 2
	f := newFixture(cfg)
	f.hotInbox.existsFlag["viewer"] = true
	for i := 1; i <= 4; i++ {
		id := string(rune('0' + i))
		f.hotInbox.inboxes["viewer"] = append(f.hotInbox.inboxes["viewer"], domain.Candidate{PostID: id, AuthorID: "prolific-author"})
		f.postMeta.byID[id] = domain.PostMeta{PostID: id, AuthorID: "prolific-author", CreatedAt: time.Now()}
		f.ranking.scoreByID[id] = float64(i)
	}

	result, err := f.svc.GetFeed(context.Background(), "viewer", "")
	if err != nil {
		t.Fatal(err)
	}
	organicCount := 0
	for _, item := range result.Items {
		if !item.IsAd {
			organicCount++
		}
	}
	if organicCount > 2 {
		t.Errorf("expected at most 2 posts from the same author, got %d", organicCount)
	}
}

func TestGetFeed_InsertsAdsAtSlotsThreeAndEight(t *testing.T) {
	cfg := defaultConfig()
	cfg.PageSize = 10
	f := newFixture(cfg)
	f.hotInbox.existsFlag["viewer"] = true
	for i := 1; i <= 10; i++ {
		id := string(rune('a' + i))
		f.hotInbox.inboxes["viewer"] = append(f.hotInbox.inboxes["viewer"], domain.Candidate{PostID: id, AuthorID: id})
		f.postMeta.byID[id] = domain.PostMeta{PostID: id, AuthorID: id, CreatedAt: time.Now()}
		f.ranking.scoreByID[id] = float64(10 - i)
	}

	result, err := f.svc.GetFeed(context.Background(), "viewer", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) < 8 {
		t.Fatalf("expected at least 8 items to check ad slots, got %d", len(result.Items))
	}
	if !result.Items[2].IsAd { // 1-indexed slot 3 = index 2
		t.Errorf("expected an ad at slot 3, got %+v", result.Items[2])
	}
	if !result.Items[7].IsAd { // 1-indexed slot 8 = index 7
		t.Errorf("expected an ad at slot 8, got %+v", result.Items[7])
	}
}

func TestGetFeed_DedupesCandidateAppearingInMultipleSources(t *testing.T) {
	f := newFixture(defaultConfig())
	f.hotInbox.existsFlag["viewer"] = true
	f.hotInbox.inboxes["viewer"] = []domain.Candidate{{PostID: "dup", AuthorID: "a", Source: "in-network"}}
	f.vector.vectorOK = true
	f.vector.vector = []float64{0.1, 0.2}
	f.vector.searchResult = []domain.Candidate{{PostID: "dup", AuthorID: "a", Source: "vector"}}
	f.postMeta.byID["dup"] = domain.PostMeta{PostID: "dup", AuthorID: "a", CreatedAt: time.Now()}

	result, err := f.svc.GetFeed(context.Background(), "viewer", "")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, item := range result.Items {
		if item.PostID == "dup" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected the duplicate candidate to appear exactly once, got %d", count)
	}
}

func TestResetSeenState_DelegatesToRepository(t *testing.T) {
	f := newFixture(defaultConfig())
	if err := f.svc.ResetSeenState(context.Background(), "viewer"); err != nil {
		t.Fatal(err)
	}
	if len(f.seenState.resetCalls) != 1 || f.seenState.resetCalls[0] != "viewer" {
		t.Errorf("expected Reset to be called with the viewer's ID, got %v", f.seenState.resetCalls)
	}
}

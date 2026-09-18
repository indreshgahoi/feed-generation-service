package service

import "feed-aggregation-service/internal/domain"

// applyDiversity enforces "max N posts per author" while preserving rank
// order -- a business rule layered ON TOP of relevance ranking, which is
// why it runs after ranking, not before. See doc/DESIGN.md "Why this order
// matters".
func applyDiversity(ranked []domain.RankedItem, maxPerAuthor int) []domain.RankedItem {
	counts := make(map[string]int)
	out := make([]domain.RankedItem, 0, len(ranked))
	for _, item := range ranked {
		if counts[item.AuthorID] >= maxPerAuthor {
			continue
		}
		counts[item.AuthorID]++
		out = append(out, item)
	}
	return out
}

type adSlot struct {
	position int // 1-indexed position in the final page
	item     domain.FeedItem
}

// insertAds implements "Dynamic Ad Insertion (Slot 3, Slot 8)": ads land
// at fixed 1-indexed positions, pushing organic content down, with the
// page still truncated to pageSize afterwards.
func insertAds(items []domain.FeedItem, pageSize int) []domain.FeedItem {
	ads := []adSlot{
		{position: 3, item: domain.FeedItem{PostID: "ad-1", AuthorID: "sponsor", Caption: "Sponsored", Source: "ad", IsAd: true}},
		{position: 8, item: domain.FeedItem{PostID: "ad-2", AuthorID: "sponsor", Caption: "Sponsored", Source: "ad", IsAd: true}},
	}

	out := make([]domain.FeedItem, 0, pageSize)
	organicIdx := 0
	for pos := 1; pos <= pageSize; pos++ {
		placedAd := false
		for _, ad := range ads {
			if ad.position == pos {
				out = append(out, ad.item)
				placedAd = true
				break
			}
		}
		if placedAd {
			continue
		}
		if organicIdx >= len(items) {
			break
		}
		out = append(out, items[organicIdx])
		organicIdx++
	}
	return out
}

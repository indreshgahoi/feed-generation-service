// Package moderation implements domain.ContentModerator as a synchronous
// keyword blocklist -- the "top keywords checked synchronously" half of
// the two-tier moderation funnel described in
// doc/engagement-at-scale.md. The async ML/NLP half of that funnel
// (toxicity classifiers, image models, sub-2s hide SLA) is NOT
// implemented here; see that doc for exactly why and what it would take.
package moderation

import "strings"

// BlocklistModerator rejects comments containing any of a fixed set of
// substrings, case-insensitive. This catches the same class of obvious,
// cheap-to-detect abuse a real bloom-filter-backed keyword check would
// (doc/engagement-at-scale.md), without needing a bloom filter at this
// repo's scale -- a few dozen banned substrings fit comfortably in a Go
// slice checked with strings.Contains.
type BlocklistModerator struct {
	blocked []string
}

func NewBlocklistModerator(blockedTerms []string) *BlocklistModerator {
	lower := make([]string, len(blockedTerms))
	for i, term := range blockedTerms {
		lower[i] = strings.ToLower(term)
	}
	return &BlocklistModerator{blocked: lower}
}

// DefaultBlocklist is intentionally tiny and obviously-illustrative --
// this is a demonstration of the synchronous pre-filter PATTERN, not a
// real trust & safety wordlist.
var DefaultBlocklist = []string{"spamlink.biz", "buy-followers-now"}

func (m *BlocklistModerator) IsAllowed(text string) bool {
	lower := strings.ToLower(text)
	for _, term := range m.blocked {
		if strings.Contains(lower, term) {
			return false
		}
	}
	return true
}

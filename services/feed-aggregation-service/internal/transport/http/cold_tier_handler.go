package http

import (
	"encoding/json"
	"net/http"

	"feed-aggregation-service/internal/domain"
)

// ColdTierHandler exposes the cold tier over HTTP so fanout-worker (Java)
// can append to it -- BadgerDB is a Go-only embedded library, so a
// cross-language write has to go through this service rather than a
// shared library. See doc/caching.md.
type ColdTierHandler struct {
	coldInbox domain.ColdInboxRepository
}

func NewColdTierHandler(coldInbox domain.ColdInboxRepository) *ColdTierHandler {
	return &ColdTierHandler{coldInbox: coldInbox}
}

type appendColdTierRequest struct {
	UserID   string `json:"userId"`
	PostID   string `json:"postId"`
	AuthorID string `json:"authorId"`
}

// Append is called by fanout-worker when fanning out a post to a DORMANT
// follower, instead of the pre-tiering behavior of simply dropping that
// fan-out write. See doc/caching.md.
func (h *ColdTierHandler) Append(w http.ResponseWriter, r *http.Request) {
	var req appendColdTierRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.PostID == "" {
		writeError(w, http.StatusBadRequest, "userId and postId are required")
		return
	}
	err := h.coldInbox.Append(r.Context(), req.UserID, domain.Candidate{
		PostID: req.PostID, AuthorID: req.AuthorID, Source: "in-network",
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to append to cold tier")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"appended": true})
}

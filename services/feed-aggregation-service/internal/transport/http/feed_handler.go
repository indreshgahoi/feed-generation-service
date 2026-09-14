package http

import (
	"encoding/json"
	"net/http"

	"feed-aggregation-service/internal/service"
)

type FeedHandler struct {
	feed *service.FeedService
}

func NewFeedHandler(feed *service.FeedService) *FeedHandler {
	return &FeedHandler{feed: feed}
}

func (h *FeedHandler) GetFeed(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userId")
	if userID == "" {
		writeError(w, http.StatusBadRequest, "userId query parameter is required")
		return
	}

	result, err := h.feed.GetFeed(r.Context(), userID, r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	items := make([]map[string]any, 0, len(result.Items))
	for _, item := range result.Items {
		m := map[string]any{
			"postId": item.PostID, "authorId": item.AuthorID, "source": item.Source, "score": item.Score,
		}
		if item.IsAd {
			m["isAd"] = true
			m["caption"] = item.Caption
		} else {
			m["mediaUrl"] = item.MediaURL
			m["caption"] = item.Caption
			m["likeCount"] = item.LikeCount
			m["commentCount"] = item.CommentCount
			m["likedByMe"] = item.LikedByMe
			m["createdAt"] = item.CreatedAt.Format("2006-01-02T15:04:05Z07:00")
		}
		items = append(items, m)
	}

	writeJSON(w, http.StatusOK, map[string]any{"items": items, "nextCursor": result.NextCursor})
}

type resetSeenRequest struct {
	UserID string `json:"userId"`
}

// ResetSeen is a LOCAL DEMO CONVENIENCE, not a real product feature -- see
// service.FeedService.ResetSeenState and README.md.
func (h *FeedHandler) ResetSeen(w http.ResponseWriter, r *http.Request) {
	var req resetSeenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "userId is required")
		return
	}
	if err := h.feed.ResetSeenState(r.Context(), req.UserID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to reset seen state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"reset": true})
}

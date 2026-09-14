package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"post-ingestion-service/internal/service"
)

type EngagementHandler struct {
	engagement *service.EngagementService
}

func NewEngagementHandler(engagement *service.EngagementService) *EngagementHandler {
	return &EngagementHandler{engagement: engagement}
}

type likeRequest struct {
	UserID string `json:"userId"`
}

func (h *EngagementHandler) Like(w http.ResponseWriter, r *http.Request) {
	h.handleLike(w, r, h.engagement.Like)
}

func (h *EngagementHandler) Unlike(w http.ResponseWriter, r *http.Request) {
	h.handleLike(w, r, h.engagement.Unlike)
}

func (h *EngagementHandler) handleLike(
	w http.ResponseWriter, r *http.Request,
	action func(ctxReq context.Context, postID, userID int64) (service.LikeResult, error),
) {
	postID, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid post id")
		return
	}
	var req likeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	userID, err := strconv.ParseInt(req.UserID, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "userId must be numeric")
		return
	}

	result, err := action(r.Context(), postID, userID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"liked": result.Liked, "likeCount": result.LikeCount})
}

type createCommentRequest struct {
	UserID string `json:"userId"`
	Body   string `json:"body"`
}

func (h *EngagementHandler) CreateComment(w http.ResponseWriter, r *http.Request) {
	postID, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid post id")
		return
	}
	var req createCommentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	authorID, err := strconv.ParseInt(req.UserID, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "userId must be numeric")
		return
	}

	comment, err := h.engagement.CreateComment(r.Context(), postID, authorID, req.Body)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, commentResponse(comment.CommentID, comment.PostID, comment.UserID, comment.Username, comment.Body, comment.CreatedAt))
}

func (h *EngagementHandler) ListComments(w http.ResponseWriter, r *http.Request) {
	postID, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid post id")
		return
	}
	comments, err := h.engagement.ListComments(r.Context(), postID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(comments))
	for _, c := range comments {
		out = append(out, commentResponse(c.CommentID, c.PostID, c.UserID, c.Username, c.Body, c.CreatedAt))
	}
	writeJSON(w, http.StatusOK, out)
}

func commentResponse(commentID, postID, userID int64, username, body string, createdAt time.Time) map[string]any {
	return map[string]any{
		"commentId": strconv.FormatInt(commentID, 10),
		"postId":    strconv.FormatInt(postID, 10),
		"userId":    strconv.FormatInt(userID, 10),
		"username":  username,
		"body":      body,
		"createdAt": createdAt.Format(time.RFC3339),
	}
}

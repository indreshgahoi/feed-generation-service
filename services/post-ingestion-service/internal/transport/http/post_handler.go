package http

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"post-ingestion-service/internal/domain"
	"post-ingestion-service/internal/service"
)

type PostHandler struct {
	posts *service.PostService
}

func NewPostHandler(posts *service.PostService) *PostHandler {
	return &PostHandler{posts: posts}
}

type createPostRequest struct {
	UserID    string `json:"userId"`
	MediaURL  string `json:"mediaUrl"`
	MediaType int16  `json:"mediaType"`
	Caption   string `json:"caption"`
}

func (h *PostHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req createPostRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	authorID, err := strconv.ParseInt(req.UserID, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "userId must be numeric")
		return
	}

	post, err := h.posts.CreatePost(r.Context(), service.CreatePostInput{
		AuthorID:  authorID,
		MediaURL:  req.MediaURL,
		MediaType: domain.MediaType(req.MediaType),
		Caption:   req.Caption,
	})
	if err != nil && post.PostID == 0 {
		writeServiceError(w, err)
		return
	}
	// post.PostID != 0 even when err != nil means the post committed but
	// the Kafka publish failed -- still return 201 (see post_service.go),
	// the client's post did succeed.
	writeJSON(w, http.StatusCreated, postResponse(post))
}

func postResponse(post domain.Post) map[string]any {
	return map[string]any{
		"postId":    strconv.FormatInt(post.PostID, 10),
		"userId":    strconv.FormatInt(post.UserID, 10),
		"mediaUrl":  post.MediaURL,
		"mediaType": int16(post.MediaType),
		"caption":   post.Caption,
		"createdAt": post.CreatedAt.Format(time.RFC3339),
	}
}

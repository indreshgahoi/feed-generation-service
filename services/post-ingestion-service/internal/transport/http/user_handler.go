package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"post-ingestion-service/internal/service"
)

type UserHandler struct {
	users   *service.UserService
	follows *service.FollowService
}

func NewUserHandler(users *service.UserService, follows *service.FollowService) *UserHandler {
	return &UserHandler{users: users, follows: follows}
}

type createUserRequest struct {
	Username string `json:"username"`
}

func (h *UserHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req createUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	user, err := h.users.CreateUser(r.Context(), req.Username)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, userResponse(user.UserID, user.Username, 0, false))
}

func (h *UserHandler) List(w http.ResponseWriter, r *http.Request) {
	users, err := h.users.ListUsers(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(users))
	for _, u := range users {
		out = append(out, userResponse(u.UserID, u.Username, u.FollowerCount, u.IsCelebrity))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *UserHandler) GetFollowing(w http.ResponseWriter, r *http.Request) {
	userID, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}
	following, err := h.follows.Following(r.Context(), userID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	ids := make([]string, 0, len(following))
	for _, id := range following {
		ids = append(ids, strconv.FormatInt(id, 10))
	}
	writeJSON(w, http.StatusOK, ids)
}

type followRequest struct {
	FollowerID string `json:"followerId"`
	FolloweeID string `json:"followeeId"`
}

func (h *UserHandler) Follow(w http.ResponseWriter, r *http.Request) {
	h.handleFollowRequest(w, r, h.follows.Follow, map[string]bool{"following": true})
}

func (h *UserHandler) Unfollow(w http.ResponseWriter, r *http.Request) {
	h.handleFollowRequest(w, r, h.follows.Unfollow, map[string]bool{"following": false})
}

func (h *UserHandler) handleFollowRequest(
	w http.ResponseWriter, r *http.Request,
	action func(ctx context.Context, followerID, followeeID int64) error,
	successBody map[string]bool,
) {
	var req followRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	followerID, err1 := strconv.ParseInt(req.FollowerID, 10, 64)
	followeeID, err2 := strconv.ParseInt(req.FolloweeID, 10, 64)
	if err1 != nil || err2 != nil {
		writeError(w, http.StatusBadRequest, "followerId and followeeId must be numeric")
		return
	}
	if err := action(r.Context(), followerID, followeeID); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, successBody)
}

func userResponse(userID int64, username string, followerCount int, isCelebrity bool) map[string]any {
	return map[string]any{
		"userId":        strconv.FormatInt(userID, 10),
		"username":      username,
		"followerCount": followerCount,
		"isCelebrity":   isCelebrity,
	}
}

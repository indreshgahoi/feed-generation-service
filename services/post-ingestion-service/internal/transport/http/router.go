package http

import "net/http"

type Handlers struct {
	User       *UserHandler
	Post       *PostHandler
	Engagement *EngagementHandler
	Upload     *UploadHandler
}

// NewRouter wires routes to handlers only -- no middleware logic beyond
// CORS lives here, and no handler does anything but parse/call/respond.
func NewRouter(h Handlers) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /v1/uploads/presign", h.Upload.Presign)

	mux.HandleFunc("POST /v1/users", h.User.Create)
	mux.HandleFunc("GET /v1/users", h.User.List)
	mux.HandleFunc("GET /v1/users/{id}/following", h.User.GetFollowing)
	mux.HandleFunc("POST /v1/follow", h.User.Follow)
	mux.HandleFunc("POST /v1/unfollow", h.User.Unfollow)

	mux.HandleFunc("POST /v1/posts", h.Post.Create)

	mux.HandleFunc("POST /v1/posts/{id}/like", h.Engagement.Like)
	mux.HandleFunc("POST /v1/posts/{id}/unlike", h.Engagement.Unlike)
	mux.HandleFunc("POST /v1/posts/{id}/comments", h.Engagement.CreateComment)
	mux.HandleFunc("GET /v1/posts/{id}/comments", h.Engagement.ListComments)

	return withCORS(mux)
}

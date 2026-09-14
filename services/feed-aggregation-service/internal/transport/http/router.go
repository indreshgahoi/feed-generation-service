package http

import "net/http"

type Handlers struct {
	Feed     *FeedHandler
	ColdTier *ColdTierHandler
}

func NewRouter(h Handlers) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("GET /v1/feed", h.Feed.GetFeed)
	mux.HandleFunc("POST /v1/feed/reset-seen", h.Feed.ResetSeen)

	// Internal, service-to-service only (called by fanout-worker) -- not
	// part of the public API surface this service exposes to the web UI.
	mux.HandleFunc("POST /internal/cold-tier/append", h.ColdTier.Append)

	return withCORS(mux)
}

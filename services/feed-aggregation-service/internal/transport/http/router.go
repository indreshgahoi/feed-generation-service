package http

import "net/http"

type Handlers struct {
	Feed *FeedHandler
}

func NewRouter(h Handlers) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("GET /v1/feed", h.Feed.GetFeed)
	mux.HandleFunc("POST /v1/feed/reset-seen", h.Feed.ResetSeen)

	// The cold-tier append call (fanout-worker -> this service) moved to
	// gRPC -- see internal/transport/grpc and doc/wire-protocols.md. This
	// HTTP mux now only serves the public, client-facing API.

	return withCORS(mux)
}

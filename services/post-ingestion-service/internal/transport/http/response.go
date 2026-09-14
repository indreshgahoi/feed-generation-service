// Package http is the transport layer: it parses requests, calls into
// the service layer, and serializes responses. It contains no business
// logic and no repository calls -- if a handler needs an if-statement
// deciding whether something is allowed, that logic belongs in
// internal/service, not here.
package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"post-ingestion-service/internal/domain"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeServiceError maps domain sentinel errors to HTTP status codes in
// one place, so handlers don't each re-implement this mapping.
func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, domain.ErrInvalidInput), errors.Is(err, domain.ErrSelfFollow):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, domain.ErrRateLimited):
		writeError(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, domain.ErrContentRejected):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func pathInt64(r *http.Request, name string) (int64, error) {
	return strconv.ParseInt(r.PathValue(name), 10, 64)
}

// withCORS allows the local sample web UI (served from a different
// origin/port) to call this API directly during local dev. Wide open on
// purpose for a local demo -- lock this down before exposing it anywhere
// real.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

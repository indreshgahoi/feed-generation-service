package http

import (
	"encoding/json"
	"net/http"
	"time"

	"post-ingestion-service/internal/storage/minio"
)

type UploadHandler struct {
	blobs *minio.BlobStore
}

func NewUploadHandler(blobs *minio.BlobStore) *UploadHandler {
	return &UploadHandler{blobs: blobs}
}

type presignRequest struct {
	UserID   string `json:"userId"`
	Filename string `json:"filename"`
}

func (h *UploadHandler) Presign(w http.ResponseWriter, r *http.Request) {
	var req presignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.Filename == "" {
		writeError(w, http.StatusBadRequest, "userId and filename are required")
		return
	}

	objectName := req.UserID + "/" + time.Now().Format("20060102T150405") + "-" + req.Filename
	url, err := h.blobs.PresignedUploadURL(r.Context(), objectName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to presign upload url")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"objectName": objectName, "uploadUrl": url})
}

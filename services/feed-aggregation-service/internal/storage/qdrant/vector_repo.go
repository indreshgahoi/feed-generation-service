// Package qdrant implements domain.VectorRepository against Qdrant's REST
// API directly (no client library needed for the two calls this service
// makes).
package qdrant

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"feed-aggregation-service/internal/domain"
)

const embeddingDim = 384

type VectorRepo struct {
	baseURL string
	client  *http.Client
}

func NewVectorRepo(baseURL string) *VectorRepo {
	return &VectorRepo{baseURL: baseURL, client: http.DefaultClient}
}

func (r *VectorRepo) postJSON(ctx context.Context, path string, body, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("qdrant request to %s failed with status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// DeriveTasteVector approximates the "user embedding" a real two-tower
// model would maintain, by averaging the embeddings of a handful of seed
// posts. See doc/DESIGN.md.
func (r *VectorRepo) DeriveTasteVector(ctx context.Context, seedPostIDs []string) ([]float64, bool) {
	if len(seedPostIDs) == 0 {
		return nil, false
	}
	ids := make([]int64, 0, len(seedPostIDs))
	for _, s := range seedPostIDs {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			ids = append(ids, n)
		}
	}
	if len(ids) == 0 {
		return nil, false
	}

	var resp struct {
		Result []struct {
			Vector []float64 `json:"vector"`
		} `json:"result"`
	}
	if err := r.postJSON(ctx, "/collections/post_embeddings/points", map[string]any{"ids": ids, "with_vector": true}, &resp); err != nil {
		return nil, false
	}
	if len(resp.Result) == 0 {
		return nil, false
	}

	avg := make([]float64, embeddingDim)
	for _, point := range resp.Result {
		for i, v := range point.Vector {
			if i < embeddingDim {
				avg[i] += v
			}
		}
	}
	n := float64(len(resp.Result))
	for i := range avg {
		avg[i] /= n
	}
	return avg, true
}

func (r *VectorRepo) Search(ctx context.Context, vector []float64, limit int) ([]domain.Candidate, error) {
	var resp struct {
		Result []struct {
			ID      int64          `json:"id"`
			Payload map[string]any `json:"payload"`
		} `json:"result"`
	}
	err := r.postJSON(ctx, "/collections/post_embeddings/points/search", map[string]any{
		"vector": vector, "limit": limit, "with_payload": true,
	}, &resp)
	if err != nil {
		return nil, err
	}

	candidates := make([]domain.Candidate, 0, len(resp.Result))
	for _, item := range resp.Result {
		authorID, _ := item.Payload["user_id"].(string)
		candidates = append(candidates, domain.Candidate{
			PostID: strconv.FormatInt(item.ID, 10), AuthorID: authorID, Source: "vector",
		})
	}
	return candidates, nil
}

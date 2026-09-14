// Package rankingclient implements domain.RankingClient against
// ranking-service (Rust) over HTTP.
package rankingclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"feed-aggregation-service/internal/domain"
)

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 5 * time.Second}}
}

type rankCandidateReq struct {
	PostID       string    `json:"postId"`
	AuthorID     string    `json:"authorId"`
	Source       string    `json:"source"`
	LikeCount    int64     `json:"likeCount"`
	CommentCount int64     `json:"commentCount"`
	CreatedAt    time.Time `json:"createdAt"`
}

type rankRequestBody struct {
	Candidates []rankCandidateReq `json:"candidates"`
}

type rankedItemResp struct {
	PostID   string  `json:"postId"`
	AuthorID string  `json:"authorId"`
	Source   string  `json:"source"`
	Score    float64 `json:"score"`
}

type rankResponseBody struct {
	Ranked []rankedItemResp `json:"ranked"`
}

func (c *Client) Rank(ctx context.Context, metas []domain.PostMeta, sourceByPostID map[string]string) ([]domain.RankedItem, error) {
	reqBody := rankRequestBody{Candidates: make([]rankCandidateReq, 0, len(metas))}
	for _, m := range metas {
		reqBody.Candidates = append(reqBody.Candidates, rankCandidateReq{
			PostID: m.PostID, AuthorID: m.AuthorID, Source: sourceByPostID[m.PostID],
			LikeCount: m.LikeCount, CommentCount: m.CommentCount, CreatedAt: m.CreatedAt,
		})
	}

	buf, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rank", bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ranking-service unavailable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("ranking-service returned status %d", resp.StatusCode)
	}

	var respBody rankResponseBody
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		return nil, err
	}

	ranked := make([]domain.RankedItem, 0, len(respBody.Ranked))
	for _, r := range respBody.Ranked {
		ranked = append(ranked, domain.RankedItem{PostID: r.PostID, AuthorID: r.AuthorID, Source: r.Source, Score: r.Score})
	}
	return ranked, nil
}

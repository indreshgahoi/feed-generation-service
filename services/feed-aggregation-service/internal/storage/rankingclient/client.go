// Package rankingclient implements domain.RankingClient against
// ranking-service (Rust) over gRPC. The wire format is deliberately
// layered: gRPC is the transport (typed contract, HTTP/2), but the
// request/response bodies are raw FlatBuffers buffers (schemas/fbs/ranking.fbs),
// not nested protobuf messages -- chosen specifically for this
// high-volume, list-heavy call to avoid Protobuf's own parse+allocate
// step. See doc/DESIGN.md.
package rankingclient

import (
	"context"
	"fmt"
	"strconv"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"feed-aggregation-service/internal/domain"
	rankingfb "feed-aggregation-service/internal/genfbs/feed/ranking"
	rankingpb "feed-aggregation-service/internal/genproto/ranking"
)

type Client struct {
	conn   *grpc.ClientConn
	client rankingpb.RankingServiceClient
}

func New(grpcAddr string) (*Client, error) {
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial ranking-service at %s: %w", grpcAddr, err)
	}
	return &Client{conn: conn, client: rankingpb.NewRankingServiceClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Rank(ctx context.Context, metas []domain.PostMeta, sourceByPostID map[string]string) ([]domain.RankedItem, error) {
	candidatesFB, err := encodeCandidates(metas, sourceByPostID)
	if err != nil {
		return nil, fmt.Errorf("encode candidates: %w", err)
	}

	resp, err := c.client.Rank(ctx, &rankingpb.RankRequest{CandidatesFb: candidatesFB})
	if err != nil {
		return nil, fmt.Errorf("ranking-service unavailable: %w", err)
	}

	return decodeRanked(resp.GetRankedFb())
}

func encodeCandidates(metas []domain.PostMeta, sourceByPostID map[string]string) ([]byte, error) {
	b := flatbuffers.NewBuilder(0)

	offsets := make([]flatbuffers.UOffsetT, 0, len(metas))
	for _, m := range metas {
		postID, err := strconv.ParseUint(m.PostID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("candidate postID %q: %w", m.PostID, err)
		}
		authorID, err := strconv.ParseUint(m.AuthorID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("candidate authorID %q: %w", m.AuthorID, err)
		}

		sourceOff := b.CreateString(sourceByPostID[m.PostID])
		rankingfb.CandidateStart(b)
		rankingfb.CandidateAddPostId(b, postID)
		rankingfb.CandidateAddAuthorId(b, authorID)
		rankingfb.CandidateAddSource(b, sourceOff)
		rankingfb.CandidateAddLikeCount(b, m.LikeCount)
		rankingfb.CandidateAddCommentCount(b, m.CommentCount)
		rankingfb.CandidateAddCreatedAt(b, uint64(m.CreatedAt.UnixMilli()))
		offsets = append(offsets, rankingfb.CandidateEnd(b))
	}

	rankingfb.CandidateListStartItemsVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	itemsVec := b.EndVector(len(offsets))

	rankingfb.CandidateListStart(b)
	rankingfb.CandidateListAddItems(b, itemsVec)
	listOff := rankingfb.CandidateListEnd(b)

	b.Finish(listOff)
	return b.FinishedBytes(), nil
}

func decodeRanked(buf []byte) ([]domain.RankedItem, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	rl := rankingfb.GetRootAsRankedList(buf, 0)

	n := rl.ItemsLength()
	ranked := make([]domain.RankedItem, 0, n)
	var item rankingfb.RankedItem
	for i := 0; i < n; i++ {
		if !rl.Items(&item, i) {
			continue
		}
		ranked = append(ranked, domain.RankedItem{
			PostID:   strconv.FormatUint(item.PostId(), 10),
			AuthorID: strconv.FormatUint(item.AuthorId(), 10),
			Source:   string(item.Source()),
			Score:    item.Score(),
		})
	}
	return ranked, nil
}

// Package kafka implements domain.EventPublisher. The `post-created`
// event is FlatBuffers-encoded, not JSON -- it's produced once here and
// deserialized independently by 3 consumers in 3 languages
// (fanout-worker, vector-pipeline, notification-service), so each of
// them gets zero-copy field access instead of a full parse. See
// doc/DESIGN.md.
package kafka

import (
	"context"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/segmentio/kafka-go"

	events "post-ingestion-service/internal/genfbs/feed/events"

	"post-ingestion-service/internal/domain"
)

const postCreatedTopic = "post-created"

type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(brokers []string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  postCreatedTopic,
			Balancer:               &kafka.Hash{}, // partition by key (author_id) for in-order delivery
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

func (p *Publisher) PublishPostCreated(ctx context.Context, post domain.Post) error {
	if post.PostID < 0 || post.UserID < 0 {
		return fmt.Errorf("post-created event requires non-negative IDs, got postId=%d userId=%d", post.PostID, post.UserID)
	}

	b := flatbuffers.NewBuilder(0)
	mediaURLOff := b.CreateString(post.MediaURL)
	captionOff := b.CreateString(post.Caption)

	events.PostCreatedEventStart(b)
	events.PostCreatedEventAddPostId(b, uint64(post.PostID))
	events.PostCreatedEventAddUserId(b, uint64(post.UserID))
	events.PostCreatedEventAddMediaUrl(b, mediaURLOff)
	events.PostCreatedEventAddMediaType(b, events.MediaType(post.MediaType))
	events.PostCreatedEventAddCaption(b, captionOff)
	events.PostCreatedEventAddCreatedAt(b, uint64(post.CreatedAt.UnixMilli()))
	eventOff := events.PostCreatedEventEnd(b)
	b.Finish(eventOff)

	// Key = author/user ID (as a decimal string, matching how IDs are
	// represented everywhere else in this service's routing logic) --
	// unchanged from the JSON wire format, so partitioning behavior
	// (all of one author's posts stay in order on one partition) is
	// identical.
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   fmt.Appendf(nil, "%d", post.UserID),
		Value: b.FinishedBytes(),
	})
}

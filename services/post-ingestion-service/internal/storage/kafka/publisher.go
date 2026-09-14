// Package kafka implements domain.EventPublisher.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"post-ingestion-service/internal/domain"
)

const postCreatedTopic = "post-created"

type postCreatedEvent struct {
	PostID    string `json:"postId"`
	UserID    string `json:"userId"`
	MediaURL  string `json:"mediaUrl"`
	MediaType int16  `json:"mediaType"`
	Caption   string `json:"caption"`
	CreatedAt string `json:"createdAt"`
}

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
	event := postCreatedEvent{
		PostID:    strconv.FormatInt(post.PostID, 10),
		UserID:    strconv.FormatInt(post.UserID, 10),
		MediaURL:  post.MediaURL,
		MediaType: int16(post.MediaType),
		Caption:   post.Caption,
		CreatedAt: post.CreatedAt.Format(time.RFC3339),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal post-created event: %w", err)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.UserID),
		Value: payload,
	})
}

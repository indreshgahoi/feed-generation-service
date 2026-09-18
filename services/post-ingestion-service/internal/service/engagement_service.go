package service

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/sync/singleflight"

	"post-ingestion-service/internal/domain"
)

type EngagementService struct {
	likes     domain.LikeRepository
	comments  domain.CommentRepository
	users     domain.UserRepository
	counters  domain.CounterRepository
	likeState domain.LikeStateRepository
	rateLimit domain.RateLimiter
	moderator domain.ContentModerator
	minter    domain.IDMinter
	clock     func() time.Time
	commentSF singleflight.Group
}

func NewEngagementService(
	likes domain.LikeRepository,
	comments domain.CommentRepository,
	users domain.UserRepository,
	counters domain.CounterRepository,
	likeState domain.LikeStateRepository,
	rateLimit domain.RateLimiter,
	moderator domain.ContentModerator,
	minter domain.IDMinter,
) *EngagementService {
	return &EngagementService{
		likes: likes, comments: comments, users: users, counters: counters,
		likeState: likeState, rateLimit: rateLimit, moderator: moderator,
		minter: minter, clock: time.Now,
	}
}

type LikeResult struct {
	Liked     bool
	LikeCount int64
}

// Like is idempotent -- liking a post twice only increments the sharded
// Redis counter once, because the Postgres row insert (which decides
// whether to increment at all) is itself idempotent via ON CONFLICT DO
// NOTHING. Rate-limited per (user, post) to guard against like/unlike
// flapping -- see doc/DESIGN.md.
func (s *EngagementService) Like(ctx context.Context, postID, userID int64) (LikeResult, error) {
	allowed, err := s.rateLimit.Allow(ctx, "like:"+strconv.FormatInt(userID, 10)+":"+strconv.FormatInt(postID, 10))
	if err != nil {
		return LikeResult{}, fmt.Errorf("like: rate limit check: %w", err)
	}
	if !allowed {
		return LikeResult{}, domain.ErrRateLimited
	}

	created, err := s.likes.Create(ctx, domain.Like{PostID: postID, UserID: userID, CreatedAt: s.clock()})
	if err != nil {
		return LikeResult{}, fmt.Errorf("like: %w", err)
	}
	if err := s.likeState.MarkLiked(ctx, userID, postID); err != nil {
		// Read-your-own-writes cache write failed; the durable Postgres
		// row is already correct, so this is a visible-lag bug, not a
		// data-loss one -- log-worthy in production, not fatal here.
		return LikeResult{}, fmt.Errorf("like: update like-state cache: %w", err)
	}

	var count int64
	if created {
		count, err = s.counters.IncrLikeCount(ctx, postID, userID)
	} else {
		count, err = s.counters.GetLikeCount(ctx, postID)
	}
	if err != nil {
		return LikeResult{}, fmt.Errorf("like: read count: %w", err)
	}
	return LikeResult{Liked: true, LikeCount: count}, nil
}

func (s *EngagementService) Unlike(ctx context.Context, postID, userID int64) (LikeResult, error) {
	allowed, err := s.rateLimit.Allow(ctx, "like:"+strconv.FormatInt(userID, 10)+":"+strconv.FormatInt(postID, 10))
	if err != nil {
		return LikeResult{}, fmt.Errorf("unlike: rate limit check: %w", err)
	}
	if !allowed {
		return LikeResult{}, domain.ErrRateLimited
	}

	deleted, err := s.likes.Delete(ctx, postID, userID)
	if err != nil {
		return LikeResult{}, fmt.Errorf("unlike: %w", err)
	}
	if err := s.likeState.MarkUnliked(ctx, userID, postID); err != nil {
		return LikeResult{}, fmt.Errorf("unlike: update like-state cache: %w", err)
	}

	var count int64
	if deleted {
		count, err = s.counters.DecrLikeCount(ctx, postID, userID)
	} else {
		count, err = s.counters.GetLikeCount(ctx, postID)
	}
	if err != nil {
		return LikeResult{}, fmt.Errorf("unlike: read count: %w", err)
	}
	return LikeResult{Liked: false, LikeCount: count}, nil
}

type CommentWithAuthor struct {
	domain.Comment
}

// CreateComment mints the comment's ID on the POST's shard (comments
// co-locate with their post, not their author -- see doc/DESIGN.md),
// so "list comments for this post" is always single-shard regardless of
// how many different shards the commenters themselves are spread across.
// Runs the synchronous moderation pre-filter before persisting -- see
// doc/DESIGN.md.
func (s *EngagementService) CreateComment(ctx context.Context, postID, authorID int64, body string) (CommentWithAuthor, error) {
	if body == "" {
		return CommentWithAuthor{}, fmt.Errorf("%w: comment body is required", domain.ErrInvalidInput)
	}
	if !s.moderator.IsAllowed(body) {
		return CommentWithAuthor{}, domain.ErrContentRejected
	}

	comment := domain.Comment{
		CommentID: s.minter.NewIDInheritingShard(postID),
		PostID:    postID,
		UserID:    authorID,
		CreatedAt: s.clock(),
		Body:      body,
	}
	if err := s.comments.Create(ctx, comment); err != nil {
		return CommentWithAuthor{}, fmt.Errorf("create comment: %w", err)
	}
	if _, err := s.counters.IncrCommentCount(ctx, postID); err != nil {
		return CommentWithAuthor{}, fmt.Errorf("create comment: increment count: %w", err)
	}

	author, err := s.users.GetByID(ctx, authorID)
	if err == nil {
		comment.Username = author.Username
	}
	return CommentWithAuthor{Comment: comment}, nil
}

// ListComments hydrates each comment with its author's username, and
// collapses concurrent identical requests for the same post's comments
// into a single underlying fetch via singleflight -- the standard fix
// for the thundering-herd/cache-stampede pattern described in
// doc/DESIGN.md ("Hot Post Cache Stampede"): when a viral
// post's comment thread is requested by many viewers within the same
// instant, only one of them actually queries the repository.
func (s *EngagementService) ListComments(ctx context.Context, postID int64) ([]CommentWithAuthor, error) {
	key := strconv.FormatInt(postID, 10)
	v, err, _ := s.commentSF.Do(key, func() (any, error) {
		comments, err := s.comments.ListByPost(ctx, postID)
		if err != nil {
			return nil, fmt.Errorf("list comments: %w", err)
		}

		usernameCache := make(map[int64]string)
		result := make([]CommentWithAuthor, 0, len(comments))
		for _, c := range comments {
			if username, ok := usernameCache[c.UserID]; ok {
				c.Username = username
			} else if author, err := s.users.GetByID(ctx, c.UserID); err == nil {
				c.Username = author.Username
				usernameCache[c.UserID] = author.Username
			}
			result = append(result, CommentWithAuthor{Comment: c})
		}
		return result, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]CommentWithAuthor), nil
}

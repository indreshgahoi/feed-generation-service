-- Posts Table (Sharded logically by user_id)
-- media_url is TEXT rather than the doc's VARCHAR(512): the local sample UI
-- stores self-contained data: URI placeholder images (no S3/MinIO upload
-- flow wired into the UI), which run longer than a real signed S3 URL would.
CREATE TABLE IF NOT EXISTS posts (
    post_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    media_url TEXT NOT NULL,
    media_type SMALLINT NOT NULL, -- 1: Image, 2: Video, 3: Carousel
    caption TEXT,
    like_count INT DEFAULT 0,
    comment_count INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_user_posts ON posts (user_id, created_at DESC);
ALTER TABLE posts ALTER COLUMN media_url TYPE TEXT;

-- Social Graph Edges (Follows)
CREATE TABLE IF NOT EXISTS follows (
    follower_id BIGINT NOT NULL,
    followee_id BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (follower_id, followee_id)
);
CREATE INDEX IF NOT EXISTS idx_followee_lookup ON follows (followee_id);

-- User Profiles & Celebrity Flagging
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username VARCHAR(64) UNIQUE NOT NULL,
    follower_count INT DEFAULT 0,
    is_celebrity BOOLEAN GENERATED ALWAYS AS (follower_count > 25000) STORED,
    last_active_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Likes -- one row per (post, user); post_id/user_id together are the PK so
-- liking twice is naturally idempotent (INSERT ... ON CONFLICT DO NOTHING).
CREATE TABLE IF NOT EXISTS likes (
    post_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (post_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_likes_user ON likes (user_id);

-- Comments -- real text, not just a counter, so this behaves like an
-- actual engagement feature rather than a fake metric.
CREATE TABLE IF NOT EXISTS comments (
    comment_id BIGSERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_comments_post ON comments (post_id, created_at);

-- Notifications, fed by notification-service on @mention parsing
CREATE TABLE IF NOT EXISTS notifications (
    notification_id BIGSERIAL PRIMARY KEY,
    recipient_user_id BIGINT NOT NULL,
    actor_user_id BIGINT NOT NULL,
    post_id BIGINT NOT NULL,
    type VARCHAR(32) NOT NULL DEFAULT 'mention',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    read_at TIMESTAMP WITH TIME ZONE
);
CREATE INDEX IF NOT EXISTS idx_notifications_recipient ON notifications (recipient_user_id, created_at DESC);

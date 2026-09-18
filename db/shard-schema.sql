-- Applied identically to all 4 shard instances (shard-0..shard-3). Which
-- shard a given row lives on is an application-level decision (see
-- pkg/sharding / doc/DESIGN.md) -- this file just defines the schema,
-- not shard-specific data.
--
-- Note: there is no `follows` table here. The social graph lives in
-- Neo4j (see doc/DESIGN.md, "The social graph lives in Neo4j, not
-- sharded Postgres") -- a follow edge genuinely straddles two shards by
-- definition, which is exactly the shape a graph database is for and a
-- sharded relational table is not.
--
-- Every PRIMARY KEY here is an application-generated Snowflake-style ID
-- (pkg/sharding.IDGenerator / com.feed.sharding.SnowflakeIdGenerator),
-- never a DB-native SERIAL/BIGSERIAL: an auto-increment sequence is local
-- to one Postgres instance, so shard-0's row #1 and shard-1's row #1
-- would collide once merged -- self-routing IDs are minted by the
-- application specifically so every ID is globally unique across all 4
-- independent databases.

-- Identity/profile facts only. Social facts (follower_count,
-- is_celebrity) live on the Neo4j User node instead.
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username VARCHAR(64) UNIQUE NOT NULL,
    last_active_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Co-located with the author's shard: a post's ID embeds the same shard
-- bits as its author's user_id (inherited at creation time, not re-hashed).
CREATE TABLE IF NOT EXISTS posts (
    post_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    media_url TEXT NOT NULL,
    media_type SMALLINT NOT NULL, -- 1: Image, 2: Video, 3: Carousel
    caption TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_user_posts ON posts (user_id, created_at DESC);

-- Co-located with the LIKING user's shard, not the post's -- see
-- doc/DESIGN.md. like_count itself lives in Redis, not a column here
-- (see doc/DESIGN.md "Counters live in Redis, not the sharded database").
CREATE TABLE IF NOT EXISTS likes (
    post_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (post_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_likes_user ON likes (user_id);

-- Co-located with the POST's shard, not the commenter's -- see
-- doc/DESIGN.md. comment_id is minted using the post's shard.
CREATE TABLE IF NOT EXISTS comments (
    comment_id BIGINT PRIMARY KEY,
    post_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_comments_post ON comments (post_id, created_at);

-- Co-located with the RECIPIENT's shard. notification_id is minted using
-- the recipient's shard.
CREATE TABLE IF NOT EXISTS notifications (
    notification_id BIGINT PRIMARY KEY,
    recipient_user_id BIGINT NOT NULL,
    actor_user_id BIGINT NOT NULL,
    post_id BIGINT NOT NULL,
    type VARCHAR(32) NOT NULL DEFAULT 'mention',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    read_at TIMESTAMP WITH TIME ZONE
);
CREATE INDEX IF NOT EXISTS idx_notifications_recipient ON notifications (recipient_user_id, created_at DESC);

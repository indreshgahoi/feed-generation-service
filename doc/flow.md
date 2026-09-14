# End-to-End Flow

Two walkthroughs: one post being written, and one feed being read, both
through the actually-sharded system. Each step names the real file so you
can jump straight to the code.

## Write path: "user_2 posts a photo mentioning @user_3"

1. **Client -> post-ingestion-service.** `POST /v1/posts` with
   `{userId, mediaUrl, mediaType, caption}`.
   [`post_handler.go`](../services/post-ingestion-service/internal/transport/http/post_handler.go)
   decodes the body; [`post_service.go`](../services/post-ingestion-service/internal/service/post_service.go)
   validates it (media type in range, required fields present).

2. **ID generation, inheriting the author's shard.** A 64-bit self-routing
   Snowflake ID is minted -- but unlike a brand-new user, a post doesn't
   get a fresh consistent-hash placement. [`IDMinter.NewIDInheritingShard`](../services/post-ingestion-service/internal/domain/repository.go)
   extracts `user_2`'s shard bits from their own `user_id` and stamps the
   same shard bits into the new post ID (`pkg/sharding/snowflake.go`'s
   `IDGenerator`, keyed by shard). No hashing, no lookup -- the post simply
   lives wherever its author already lives, so "get this user's posts" and
   "get this post" both stay single-shard forever.

3. **Synchronous Postgres commit, to that one shard.** [`post_repo.go`](../services/post-ingestion-service/internal/storage/postgres/post_repo.go)
   resolves `user_2`'s shard via [`ShardedPool.PoolForExistingID`](../services/post-ingestion-service/internal/storage/postgres/pool.go)
   (a bit-shift, not a lookup) and runs a single `INSERT INTO posts (...)`
   against that shard's Postgres instance. This is the only synchronous
   write in the whole path -- everything after this point is
   fire-and-forget from the client's perspective.

4. **Kafka publish.** [`publisher.go`](../services/post-ingestion-service/internal/storage/kafka/publisher.go)
   marshals a `post-created` JSON event (`postId, userId, mediaUrl,
   mediaType, caption, createdAt`) and publishes it to the `post-created`
   topic, **keyed by `userId`**. The handler returns `201 Created`
   immediately after -- it does not wait for any consumer to process the
   event. (If the Kafka publish itself fails, the code logs a loud warning
   and still returns 201, since the post is already durably committed to
   its shard; a real system would use a transactional outbox here instead.)

   From this point, three consumer groups independently read the same
   event off the topic. They don't know about each other and can't block
   each other.

5a. **fanout-worker reacts** ([`FanoutService.java`](../services/fanout-worker/src/main/java/com/feed/fanout/service/FanoutService.java)):
   - Read `isCelebrity` off `user_2`'s **Neo4j** node
     ([`Neo4jSocialGraphRepository.java`](../services/fanout-worker/src/main/java/com/feed/fanout/storage/neo4j/Neo4jSocialGraphRepository.java))
     -- not a Postgres column; the social graph, including this derived
     flag, lives entirely in the graph store (see [sharding.md](sharding.md)).
   - **Not a celebrity (the common case):** query Neo4j for every
     follower of `user_2` (`MATCH (follower)-[:FOLLOWS]->(user_2)`), then
     partition those follower IDs by shard using the self-routing bit-shift
     and run one concurrent, per-shard Postgres query
     ([`PostgresActivityRepository.java`](../services/fanout-worker/src/main/java/com/feed/fanout/storage/postgres/PostgresActivityRepository.java),
     backed by [`ShardedDataSource.java`](../services/fanout-worker/src/main/java/com/feed/fanout/storage/postgres/ShardedDataSource.java))
     to split followers into active (within `ACTIVE_WITHIN_DAYS`) vs.
     dormant. Any follower ID that resolves to an out-of-range shard is
     dropped and logged rather than failing the whole fan-out -- see
     [learning-notes.md](learning-notes.md) bug #6 for why that matters.
   - Active followers: `ZADD feed:user:<follower_id> <created_at_ms>
     <post_id>` into Redis ([`RedisHotInboxRepository.java`](../services/fanout-worker/src/main/java/com/feed/fanout/storage/redis/RedisHotInboxRepository.java)),
     then trimmed back to `FEED_INBOX_MAX_ITEMS` (800) if it grew past
     that.
   - Dormant followers: instead of being dropped (the pre-tiering
     behavior), the same `(post_id, score)` entry is sent via HTTP to
     feed-aggregation-service's `POST /internal/cold-tier/append`
     ([`ColdTierHttpClient.java`](../services/fanout-worker/src/main/java/com/feed/fanout/storage/http/ColdTierHttpClient.java)),
     which appends it to that follower's BadgerDB cold-tier entry. See
     [caching.md](caching.md) for the full hot/cold design.
   - **Celebrity path (not triggered here, since `user_2` isn't one):** a
     single `ZADD celebrity:outbox:2 ...` -- no per-follower work, no
     Neo4j follower-list query, no cross-shard Postgres call at all. This
     is the entire point of the hybrid design: a celebrity's follower
     count never touches fan-out latency.
   - Kafka offsets are committed manually, only after the Redis/HTTP
     writes succeed, so a crash mid-fan-out reprocesses the event rather
     than silently dropping it (at-least-once, not exactly-once -- a
     re-run just re-`ZADD`s the same member/score, which is idempotent).

5b. **vector-pipeline reacts** ([`main.py`](../services/vector-pipeline/main.py)):
   - Runs the caption through `all-MiniLM-L6-v2` (Sentence-Transformers)
     to get a 384-dim embedding.
   - Upserts it into Qdrant's `post_embeddings` collection, point ID =
     `post_id`, payload = `{user_id, created_at, caption}`.
   - Commits its own Kafka offset independently of the other two
     consumers. Untouched by sharding -- it never talks to Postgres.

5c. **notification-service reacts** ([`notificationService.ts`](../services/notification-service/src/service/notificationService.ts)):
   - Regexes the caption for `@(\w+)` mentions -> finds `user_3`.
   - Looks up `user_3`'s `user_id` via the **Redis username directory**
     ([`usernameDirectory.ts`](../services/notification-service/src/storage/redis/usernameDirectory.ts)),
     not a Postgres query -- there's no single `users` table to query
     anymore, and username isn't the shard key, so this is the same
     global-secondary-index problem [sharding.md](sharding.md#the-username-problem-a-global-secondary-index)
     names.
   - Mints the notification's ID **inheriting `user_3`'s shard** using a
     BigInt Snowflake generator ([`snowflake.ts`](../services/notification-service/src/storage/sharding/snowflake.ts) --
     BigInt because these IDs exceed `Number.MAX_SAFE_INTEGER`), and
     `INSERT`s into `user_3`'s shard via [`notificationRepository.ts`](../services/notification-service/src/storage/postgres/notificationRepository.ts).

At this point the post is durable on its author's shard, fanned out to
active followers' hot inboxes (with dormant followers safely in the cold
tier instead of dropped), embedded for vector recall, and any mentions
have produced a notification on the right shard -- all without the
original HTTP request waiting on any of it.

## Read path: "user_7 requests their feed"

`GET /v1/feed?userId=7&cursor=...` into
[`feed_handler.go`](../services/feed-aggregation-service/internal/transport/http/feed_handler.go),
which delegates to [`FeedService.GetFeed`](../services/feed-aggregation-service/internal/service/feed_service.go)
and runs the following stages:

**Stage 1 -- fan-in candidate retrieval, concurrently (3 goroutines):**
- *In-network:* `ZREVRANGE feed:user:7 0 499 WITHSCORES` against the
  **hot tier** ([`hot_inbox_repo.go`](../services/feed-aggregation-service/internal/storage/redis/hot_inbox_repo.go)).
  If this comes back empty, that's a dormancy signal: fall back to the
  **cold tier** ([`cold_inbox_repo.go`](../services/feed-aggregation-service/internal/storage/badger/cold_inbox_repo.go),
  BadgerDB), and if it has entries, copy them into a fresh Redis ZSET
  before proceeding -- user_7 is reading right now, so they're active
  again, and the hot tier should own their data going forward. Full
  reasoning: [caching.md](caching.md).
- *Celebrity:* look up which celebrities user_7 follows via **Neo4j**
  ([`graph_repo.go`](../services/feed-aggregation-service/internal/storage/neo4j/graph_repo.go)),
  then `ZREVRANGE` each of their `celebrity:outbox:<id>` (budget split
  evenly across however many celebrities are followed, capped at
  `CELEBRITY_CANDIDATE_LIMIT` total).
- *Vector:* derive a "taste vector" by averaging the Qdrant embeddings of
  user_7's own + followed users' most recent posts. Getting those
  followees' posts is itself a cross-shard read: the followee ID list
  (from Neo4j) is grouped by shard via bit-shift and fanned out
  concurrently against each shard's Postgres, then merged -- Neo4j
  removes the graph-traversal cross-shard problem, not this one, since
  post storage is still sharded by author. Then an ANN search against the
  resulting taste vector returns up to `VECTOR_CANDIDATE_LIMIT` (250)
  semantically similar posts via [`vector_repo.go`](../services/feed-aggregation-service/internal/storage/qdrant/vector_repo.go).

  All three run in parallel -- none of them depend on each other's
  results, so there's no reason to serialize them.

**Stage 2 -- merge + dedupe:** the three candidate lists are concatenated
(in-network first, then celebrity, then vector) into one ordered list,
skipping any `post_id` already seen from an earlier source. Order matters
here only as a tie-breaker for which `source` label sticks if the same
post somehow appears in two lists.

**Stage 3 -- seen-state filter:** for every candidate, check
`ZSCORE seen:user:7 <post_id>` via [`seen_state_repo.go`](../services/feed-aggregation-service/internal/storage/redis/seen_state_repo.go).
If it has a score and that timestamp is within `SEEN_STATE_TTL_SECONDS`
(48h), drop it -- the user was already shown this post recently. (The doc
calls for a Bloom filter here for O(1) memory regardless of history
length; a Redis ZSET is easier to inspect locally. See
[trade-offs.md](trade-offs.md) for why that doesn't scale.)

**Stage 4 -- hydrate metadata, grouped by shard:** [`post_meta_repo.go`](../services/feed-aggregation-service/internal/storage/postgres/post_meta_repo.go)'s
`groupByShard` helper splits the surviving candidate IDs by shard (bit
shift, no lookup) and issues one `WHERE post_id = ANY($1)` query per shard
concurrently for `caption, media_url, created_at`, merging the results --
not one query per candidate, and not one giant cross-shard query, since
Postgres can't do that across independent instances. Any ID whose
computed shard is out of range is dropped from its batch with a logged
warning rather than failing the whole hydration (the fix for the crash
documented in [learning-notes.md](learning-notes.md) bug #6).
`like_count`/`comment_count` are fetched separately, from the **sharded
Redis counters** ([`counter_repo.go`](../services/feed-aggregation-service/internal/storage/redis/counter_repo.go)),
not from Postgres at all -- see [engagement-at-scale.md](engagement-at-scale.md).

**Stage 5 -- ranking:** the hydrated candidates are POSTed to
`ranking-service`'s `/rank` endpoint via [`rankingclient/client.go`](../services/feed-aggregation-service/internal/storage/rankingclient/client.go)
as `{postId, authorId, source, likeCount, commentCount, createdAt}`
tuples. Rust computes `P(Like)/P(Comment)/P(Share)/P(Dwell)/P(Hide)` from
recency decay + normalized engagement counts (see
[`main.rs`](../services/ranking-service/src/main.rs)), combines them into
the doc's weighted composite score, and returns everything sorted
descending. This service is entirely shard-agnostic -- it never sees a
user_id or post_id's shard, only pre-hydrated scoring inputs.

**Stage 6 -- diversity + pagination window:** [`diversity.go`](../services/feed-aggregation-service/internal/service/diversity.go)
walks the ranked list keeping at most 2 posts per author (dropping the
rest, preserving rank order otherwise), then the AES-256-GCM cursor's
offset ([`cursor.go`](../services/feed-aggregation-service/internal/service/cursor.go)) slices out this page.

**Stage 7 -- ad insertion:** the same diversity pass splices synthetic
sponsored items into 1-indexed slots 3 and 8 of the returned page,
pushing organic posts down without ever exceeding `FEED_PAGE_SIZE` (20)
total items.

**Stage 8 -- likedByMe, mark seen, respond:** each returned item's
`likedByMe` is a batch read against the **read-your-own-writes Redis Set**
(`user_likes:7`, via [`liked_repo.go`](../services/feed-aggregation-service/internal/storage/redis/liked_repo.go)) --
not a per-shard Postgres query. Every organic post_id actually returned
gets `ZADD seen:user:7 <now_ms> <post_id>`, so a repeat call for the same
user won't show them again until the 48h window lapses. The response
includes a fresh AES-256-GCM-encrypted `nextCursor` encoding the new
offset, for the client's next page request.

### Why this order matters

Seen-state filtering happens *before* ranking, not after -- there's no
point spending a network round-trip to `ranking-service` scoring posts
that are about to be thrown away for having already been shown. Diversity
and ad rules happen *after* ranking, because they're business rules
layered on top of a relevance ordering, not part of relevance itself: you
want the single best post per over-represented author to survive the
diversity cut, which requires already knowing the rank order first.
Hydration is grouped by shard *before* any Postgres call is issued, not
per-candidate, because a naive "query per post_id" loop would mean up to
~750 individual round-trips to up to 4 different Postgres instances
instead of at most 4 concurrent batch queries.

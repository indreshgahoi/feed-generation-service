# Architecture

## Goals (from the design doc)

The [source design doc](instagram_feed_design_doc.pdf) specs a feed system for
500M DAU at ~80k read QPS, p99 < 200ms, using:
- **horizontal sharding by `user_id`** (§2),
- an **asymmetric hybrid fan-out** write path (push for normal users, pull for celebrities),
- a **Meta TAO-style graph store** for the social graph (§2),
- a **Kafka event bus** decoupling post creation from fan-out/vector-indexing/notifications,
- an **in-memory timeline cache backed by RocksDB/Redis on Flash** for the cold tier (§2),
- **vector-based out-of-network recall** (Qdrant/HNSW) alongside the in-network feed,
- a **multi-stage ranking funnel** (candidate generation -> pruning -> scoring -> business rules).

This repo implements the same shape of system, at a scale that runs on one
laptop, with each service written in a different language matching the
doc's own (explicit or implied) choices. Unlike an earlier version of this
repo, sharding, the graph store, and hot/cold feed storage are **actually
implemented**, not simplified away and left as a discussion point --
see [sharding.md](sharding.md) and [caching.md](caching.md) for the design
records. The "why a different language per service" framing is explained
in the top-level [README](../README.md); this doc is about what the system
*does*, not why it's polyglot.

## Component diagram

```
                        ┌─────────────────────────────────────────────┐
                        │                  web-ui (:5173)               │
                        │   static HTML/JS, serve.py reverse-proxies    │
                        │   /api/ingest -> :4001, /api/feed -> :4002    │
                        └───────────────────┬───────────────────────────┘
                                            │
                    ┌───────────────────────┼───────────────────────┐
                    ▼                                                ▼
   ┌────────────────────────────┐                    ┌──────────────────────────────┐
   │ post-ingestion-service (Go) │                    │ feed-aggregation-service (Go) │
   │        :4001                │                    │           :4002               │
   └──┬─────────┬─────────┬──────┘                    └───┬───────┬───────┬───────┬───┘
      │         │         │                               │       │       │       │
      ▼         ▼         ▼                               ▼       ▼       ▼       ▼
  Postgres    Neo4j     Redis                         Redis    Redis   BadgerDB  Qdrant
  shard 0-3  (social   (sharded                        hot     celeb    cold     (ANN on
  (posts,    graph:    counters,                       inbox   outbox   tier     taste
  users,     FOLLOWS,  rate limit,                    (ZSET)   (ZSET)  (dormant  vector)
  likes,     follower  read-your-                                      followers)
  comments)  Count,    own-writes)
             isCeleb)     │                                     ▲
                          ▼                                     │ HTTP append
                        Kafka                          ┌────────┴────────┐
                     "post-created"                     │  (fanout-worker  │
                     3 partitions,                       │   writes here    │
                     key=author_id                       │   for dormant     │
                          │                               │   followers)      │
        ┌─────────────────┼──────────────────┐            └───────────────────┘
        ▼                 ▼                  ▼
 fanout-worker      vector-pipeline    notification-service
 (Java, consumer    (Python, consumer  (Node/TS, consumer
  group "fanout-     group "vector-     group "notification-
  worker")           pipeline")        service")
        │                 │                  │
        ▼                 ▼                  ▼
 Neo4j "who         Qdrant upsert     Redis username directory
 follows me" ->     (384-dim MiniLM   (lookup) -> Postgres
 cross-shard        embedding of      INSERT into notifications
 Postgres active-   caption)          (recipient's shard, BigInt
 follower filter                      self-routing ID)
 -> Redis hot
 inbox / celebrity
 outbox / BadgerDB
 cold tier (HTTP)

                        ┌──────────────────────────┐
                        │   ranking-service (Rust)   │
                        │           :4003             │
                        │  POST /rank -- stateless,    │
                        │  called synchronously by     │
                        │  feed-aggregation-service    │
                        └──────────────────────────┘
```

## Services

| Service | Language | Port | Owns | Talks to |
|---|---|---|---|---|
| [post-ingestion-service](../services/post-ingestion-service) | Go | 4001 | Writing posts, presigning uploads, the social graph (follow/unfollow, user list), engagement (like/unlike, comments), shard-aware ID minting | 4x Postgres shards, Neo4j, Redis (counters/rate-limit/like-state), Kafka (producer), MinIO |
| [fanout-worker](../services/fanout-worker) | Java | -- (consumer only) | Hybrid fan-out decision + write, across shards and the graph store | Kafka (consumer), Neo4j (read follower list/isCelebrity), 4x Postgres shards (cross-shard active-follower filter), Redis (hot inbox/outbox write), feed-aggregation-service (HTTP, cold-tier append) |
| [vector-pipeline](../services/vector-pipeline) | Python | -- (consumer only) | Caption embeddings | Kafka (consumer), Qdrant (write) |
| [notification-service](../services/notification-service) | Node/TS | -- (consumer only) | @mention notifications, shard-aware | Kafka (consumer), Redis (username directory read), Postgres shard owning the recipient (write) |
| [feed-aggregation-service](../services/feed-aggregation-service) | Go | 4002 | The entire read path, hot/cold feed storage | Redis, BadgerDB (embedded), 4x Postgres shards (cross-shard hydration), Neo4j (celebrity lookups), Qdrant, ranking-service (HTTP) |
| [ranking-service](../services/ranking-service) | Rust | 4003 | Stateless scoring | Nothing -- pure function over its input, no DB/cache of its own |
| [web-ui](../web-ui) | static HTML/JS + Python proxy | 5173 | Sample client | post-ingestion-service, feed-aggregation-service (via same-origin proxy) |

Each consumer service is independently scalable in principle (more Kafka
partitions + more consumer instances in the same group), though this repo
runs exactly one instance of each application service (the underlying data
stores -- Postgres, Neo4j -- are the pieces that are actually
horizontally scaled here, at 4 and 1 instance respectively).

## Data model

### Postgres -- 4 independent shards, identical schema (`db/shard-schema.sql`)

```
users            user_id PK (self-routing), username UNIQUE, last_active_at
                 -- follower_count / is_celebrity are NOT here; they live
                 -- on the Neo4j User node instead (see below)
posts            post_id PK (self-routing, inherits author's shard bits),
                 user_id, media_url, media_type, caption, created_at
                 index: (user_id, created_at DESC)
likes            post_id, user_id (composite PK -- naturally idempotent)
                 -- sharded by the LIKING user, not the post; the count
                 -- itself lives in Redis, not a like_count column
                 index: user_id
comments         comment_id PK (self-routing, inherits the POST's shard),
                 post_id, user_id, body, created_at
                 index: (post_id, created_at)
notifications    notification_id PK (self-routing, inherits RECIPIENT's shard),
                 recipient_user_id, actor_user_id, post_id, type,
                 created_at, read_at
                 index: (recipient_user_id, created_at DESC)
```

There is deliberately **no `follows` table** -- a follow edge connects two
users who can live on any two of the four shards, which is exactly the
shape a sharded relational table handles badly and a graph database
handles natively. See [sharding.md](sharding.md#the-social-graph-lives-in-neo4j-not-sharded-postgres)
for the full reasoning, including the dual-write design that was tried
first and explicitly abandoned.

Every primary key above is an application-generated 64-bit Snowflake-style
ID (`pkg/sharding.IDGenerator` / `com.feed.sharding.SnowflakeIdGenerator` /
the TypeScript BigInt equivalent), **never** a database-native
`SERIAL`/`BIGSERIAL`: an auto-increment sequence is local to one Postgres
instance, so shard-0's row #1 and shard-1's row #1 would collide once
results from both are merged. Self-routing IDs are minted by the
application specifically so every ID is globally unique across all 4
independent databases, and so that routing an *existing* row to its shard
is a bit-shift, never a lookup. Full design record: [sharding.md](sharding.md).

`likes`/`comments` are real tables with real rows (not just counters) --
they're the system of record for "did this user like this post" and "what
did they say," but the fast-moving **aggregate counts** read on every feed
request live in Redis instead of a `like_count`/`comment_count` column, for
the cross-shard reason explained in
[sharding.md](sharding.md#counters-live-in-redis-not-the-sharded-database)
and the hot-key/contention reasons explained in
[engagement-at-scale.md](engagement-at-scale.md). `ranking-service` and the
feed hydration step read counts from Redis, not from Postgres.

`media_url` is `TEXT` rather than a fixed-length `VARCHAR`: the sample UI
stores self-contained SVG `data:` URIs as placeholder images (no real
object storage wired into the UI), which run longer than a real signed S3
URL would.

### Neo4j -- the social graph (`docker-compose.yml`'s `graph-db` service)

```cypher
(:User {userId, username, followerCount, isCelebrity})
      -[:FOLLOWS {createdAt}]->
(:User)
```

- "Who does X follow": `MATCH (:User {userId:$x})-[:FOLLOWS]->(f) RETURN f`
- "Who follows Y" (fan-out's hot path): `MATCH (follower)-[:FOLLOWS]->(:User {userId:$y}) RETURN follower`

`followerCount` and `isCelebrity` are properties on the `User` node,
updated in the *same* Cypher transaction that creates or deletes a
`FOLLOWS` edge -- one graph database, one transaction, no cross-store
consistency problem for this specific fact. A lightweight `User` node is
`MERGE`d into Neo4j by post-ingestion-service in the same request that
creates the user in sharded Postgres; that's still two independent writes
to two independent systems with no shared transaction (see
[sharding.md](sharding.md#what-still-has-to-stay-in-sync) for the named,
unfixed drift window this creates). One Neo4j instance, no replication --
a real deployment would run a causal cluster; see
[sharding.md](sharding.md#what-this-doesnt-solve).

### Redis key namespaces

| Key pattern | Type | Written by | Read by | Purpose |
|---|---|---|---|---|
| `feed:user:<id>` | ZSET, score=post created-at (ms) | fanout-worker | feed-aggregation-service | Hot-tier in-network timeline inbox for active users, capped at `FEED_INBOX_MAX_ITEMS` (800) |
| `celebrity:outbox:<author_id>` | ZSET, score=post created-at (ms) | fanout-worker | feed-aggregation-service | One outbox per celebrity, pulled at read time by their followers |
| `seen:user:<id>` | ZSET, score=last-shown-at (ms) | feed-aggregation-service | feed-aggregation-service | Seen-state dedupe; entries older than `SEEN_STATE_TTL_SECONDS` (48h) are treated as unseen again |
| `likes:count:<postId>:<0..15>` | STRING (counter) | post-ingestion-service | post-ingestion-service, feed-aggregation-service | One of 16 sharded sub-counters per post, keyed by `hash(likerUserId) % 16` -- see [engagement-at-scale.md](engagement-at-scale.md) |
| `likes:count:<postId>:sum` | STRING (cached sum), 5s TTL | post-ingestion-service (on miss) | post-ingestion-service, feed-aggregation-service | Cached sum of the 16 sub-counters, so a hot read path doesn't `MGET` 16 keys every time |
| `user_likes:<userId>` | SET | post-ingestion-service | post-ingestion-service, feed-aggregation-service | Read-your-own-writes cache: "which posts has this user liked," read directly instead of querying sharded Postgres |
| `username:<name>` | STRING -> user_id | post-ingestion-service | post-ingestion-service, notification-service | Global secondary index for username lookups (username isn't the shard key, so there's no bit-shift shortcut) -- see [sharding.md](sharding.md#the-username-problem-a-global-secondary-index) |

`POST /v1/feed/reset-seen` (feed-aggregation-service) just `DEL`s a user's
`seen:user:<id>` key wholesale. It exists purely so this repo's small,
fixed sample content pool can be re-explored on demand -- a real deployment
never needs it, since real posts continuously refill the unseen candidate
pool.

### BadgerDB -- the cold feed-storage tier

One embedded, pure-Go LSM-tree instance, owned by feed-aggregation-service,
storing the same `(post_id, score)` fan-out entries the Redis hot inbox
holds -- but for followers who haven't been active recently, instead of
dropping those fan-out writes entirely. Promoted back into a fresh Redis
ZSET the moment that user reads their feed again. Full design record,
including why BadgerDB stands in for the doc's named RocksDB (and the
CockroachDB/Pebble precedent for that exact substitution): [caching.md](caching.md).

### Kafka

- Topic `post-created`, 3 partitions, key = `author_id` (so all of one
  author's posts stay in order on one partition -- matters if you ever add
  ordering-sensitive logic like "delete cancels a pending fan-out").
- Three independent consumer groups read the same topic: `fanout-worker`,
  `vector-pipeline`, `notification-service`. Each gets its own copy of every
  message and its own offset -- that's the whole point of Kafka over a
  plain queue here: one producer, N independent readers, no coordination
  between them.

### Qdrant

- Collection `post_embeddings`, 384-dim vectors (MiniLM), cosine distance.
- Point ID = post_id, payload = `{user_id, created_at, caption}`.
- No collection for user embeddings -- see [flow.md](flow.md) for how
  feed-aggregation-service derives a "taste vector" on the fly instead of
  maintaining one. Building that taste vector requires posts by a user's
  followees, which is itself a cross-shard scatter-gather (group returned
  followee IDs by shard, fan out concurrently, merge) -- one of the
  cross-shard queries [sharding.md](sharding.md#cross-shard-fan-out-queries-the-ones-that-remain)
  names as remaining even after Neo4j removes the graph-traversal ones.

## Config

Every service reads its config from environment variables, all defined in
one shared [`.env`](../.env.example) file (see that file for the full list
with defaults), plus the shared [`config/shards.json`](../config/shards.json)
file every language's shard router reads (connection string per shard,
virtual-node count for the consistent-hash ring). There's no per-service
config file format to learn beyond that -- `os.Getenv` / `System.getenv` /
`os.environ` / `process.env` / `std::env::var`, one line each, same
variable names across the stack where they overlap (e.g. `SHARD_CONFIG_PATH`,
`KAFKA_BROKERS`, `NEO4J_URI`).

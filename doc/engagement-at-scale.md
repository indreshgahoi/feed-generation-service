# Likes & Comments at Scale

Likes and comments look like the same feature ("engage with a post") but
are opposite engineering problems: likes are high-throughput, low-entropy
toggles where the hard part is write contention on a hot counter;
comments are lower-throughput, high-entropy text where the hard part is
data modeling, ranking, and moderation, not counter contention. This doc
records what of the hyperscale design for each is actually implemented
here, what's a smaller stand-in for it, and what's explicitly out of
scope with the reasoning for each -- the same standard the rest of
`doc/` holds itself to.

## Scale this is designed against

At ~500M DAU: ~5B like/unlike toggles/day (~60k/s sustained, 250k/s
peak on a viral post), ~500M comments/day (~6k/s sustained, 25k/s peak).
Read:write on interaction counters exceeds 50:1. Two different
consistency needs: the global like count can be eventually consistent
(off by a few hundred on a post with a million likes is fine); "does the
person looking at their own screen right now see their own like" cannot
be -- that one needs read-your-own-writes, immediately.

## Likes: what's implemented

**The bottleneck a naive design hits:** `UPDATE posts SET like_count =
like_count + 1 WHERE post_id = :id` serializes every concurrent like on
one row's lock. At 250k/s on one viral post, that's the whole system's
failure mode, not a slow query.

This repo already moved `like_count` out of Postgres entirely (see
`doc/sharding.md`, for an unrelated reason -- the liker and the post's
author are frequently on different shards). That sidesteps the row-lock
problem by construction: there's no row to lock. What it doesn't
sidestep is a **single Redis key** becoming the hot spot instead of a
single Postgres row -- Redis is single-threaded per command, and in a
real Redis Cluster a single key lives on exactly one node, so one very
hot key is one node doing all the work while N-1 sit idle.

**Implemented:** sharded counters, for real. `internal/storage/redis/counter_repo.go`
splits each post's like count across 16 sub-keys
(`likes:count:<postId>:<0..15>`), selecting the sub-key by
`hash(likerUserId) % 16` on write, and sums-with-a-5-second-cache on
read. A `GetLikeCount` during the read-heavy majority of requests is one
`GET` against the cached sum, not 16 reads -- matching the 50:1+
read:write ratio this is designed around.

**Implemented:** the read-your-own-writes cache. `internal/storage/redis/like_state_repo.go`
maintains `user_likes:<userId>`, a Redis Set, updated on every
like/unlike. `feed-aggregation-service`'s `LikedRepository` reads
straight from this (see `internal/storage/redis/liked_repo.go`) instead
of querying sharded Postgres -- one Redis round trip, correct regardless
of which shard the viewer or the post's author live on.

**Implemented:** rate limiting against like/unlike flapping.
`internal/storage/redis/rate_limiter.go` is a fixed-window counter
(20 toggles/user/post/minute) guarding `Like`/`Unlike` in
`EngagementService` -- the backstop against the "bots or impatient users
tapping like/unlike repeatedly" scenario.

**Deliberately NOT implemented: the async Kafka batch-flush to Postgres.**
The reference design still keeps `posts.like_count` in Postgres as a
system-of-record column, kept eventually consistent via a Kafka consumer
batching deltas into periodic bulk `UPDATE`s. This repo doesn't need that
pipeline because it made a more aggressive choice one layer up: it never
materializes the counter in Postgres **at all** -- Redis is the sole
source of truth for the count, full stop. That's a real, named trade-off,
not a simplification of the flush pipeline: if Redis's AOF is lost, the
count resets to zero with no Postgres copy to recover from, whereas the
reference design's flushed value survives a Redis loss. Worth reproducing
if this were real: either add the flush pipeline back, or accept the
count as a pure best-effort cache and treat that data-loss mode as
acceptable product risk (many products do -- a like count resetting is
usually not treated as ledger-grade data).

## Comments: what's implemented

**Sharding by `post_id`, not the commenter:** the reference design's
schema comment (`-- Partitioning Key` on `post_id`) is exactly the
decision this repo already made independently for its own reasons (see
`doc/sharding.md`, "comments co-locate with the post's shard") -- both
arrived at the same place because "list comments for post X" is the
dominant read pattern either way. No change needed here; worth naming
that convergence as a sanity check on the original design decision.

**Implemented:** a synchronous moderation pre-filter.
`internal/storage/moderation/blocklist.go` rejects comments containing
blocked substrings before they're persisted -- the "top keywords checked
synchronously" half of the reference design's two-tier moderation funnel.

**Implemented:** cache-stampede protection on reads. `EngagementService.ListComments`
wraps the repository call in `golang.org/x/sync/singleflight`, so if a
viral post's comment thread is requested by many viewers in the same
instant, only one of them actually queries the database -- the standard
fix for the "Hot Post Cache Stampede" scenario, and the idiomatic Go tool
for it (no bespoke locking).

**Deliberately NOT implemented, and why:**
- **Two-level threading (`parent_comment_id`, replies).** A real schema
  and API change (a reply endpoint, "top-level vs. replies" as separate
  fetches) that would also need web UI work to be worth shipping half-
  finished -- named explicitly here rather than silently dropped, see
  [trade-offs.md](trade-offs.md) for where this fits against everything
  else still simplified.
- **Comment ranking** (`w1*like_count + w2*verified + w3*followed -
  decay`). Needs comment-level likes and a "verified author" concept
  neither of which exist in this repo; chronological order is what's
  implemented instead.
- **Async ML/NLP toxicity moderation** (the second half of the
  moderation funnel, with a <2s hide SLA). Needs an actual model; the
  synchronous blocklist above is the illustrative stand-in for the
  pattern, not a real trust & safety system.
- **Live-video adaptive comment sampling.** Not applicable -- this repo
  has no live video feature for it to apply to.

## Why counter sharding uses a different hash than Postgres sharding

`counterShard(userId) = hash(userId) % 16` in the Redis counter code and
the consistent-hash ring in `pkg/sharding` (used to place a user on one
of 4 Postgres shards) are two unrelated schemes, deliberately not
unified. They solve different problems: one decides which of 4 physical
databases owns a row, permanently, for that row's whole lifetime; the
other decides which of 16 Redis keys absorbs one counter increment,
disposably, recomputed from scratch on every write. Sharing a scheme
between them would be a coincidence, not a simplification -- changing the
Postgres shard count should never silently change how like counters
distribute, and vice versa.

# Trade-offs: This Repo vs. Full Production Scale

This doc is written to be read on its own, ahead of a system design
interview. Every row is a real decision made while building this repo,
next to what an actual large-scale feed team would do instead, and why.
The goal isn't "this repo is wrong" -- it's "here's the reasoning an
interviewer wants to hear when you explain why you'd do it differently at
scale."

An earlier version of this table had "single Postgres instance" and
"Postgres table + Redis-cached joins for the graph" as its top two rows.
Both are gone now -- application-level sharding across 4 Postgres
instances and a real Neo4j graph store are actually implemented, not just
discussed (see [sharding.md](sharding.md)), and a Redis/BadgerDB hot-cold
feed tier replaced flat Redis (see [caching.md](caching.md)). The table
below covers what's *still* simplified relative to full production scale,
plus a few new rows the sharded/polyglot design itself introduces.

## The full table

| # | Decision | This repo | Production-grade | Why production wins at scale |
|---|---|---|---|---|
| 1 | Shard count & rebalancing | 4 fixed physical Postgres shards, no resharding tooling | Vitess/CockroachDB-style automatic shard splitting, or a manually-run but tooled resharding process, at dozens-to-hundreds of shards | This repo's ID scheme (8-bit shard ID, independent of instance count) makes growing shard count *tractable*, but there's no tooling here that actually executes a shard split/move -- see [sharding.md](sharding.md#what-this-doesnt-solve). At 500M DAU, 4 shards each carry ~125M users' worth of data; real systems need enough shards that losing one is a rounding error, and a way to add more without downtime |
| 2 | Graph store topology | Single Neo4j instance, no replication | Meta TAO or a Neo4j causal cluster -- purpose-built, horizontally scaled graph/association store | One instance is a single point of failure for the *entire* social graph -- lose it and no one can follow, unfollow, or fan out a post to anyone. TAO specifically also adds a read-through cache tier in front of the graph store tuned for the read:write ratio graph edges have (read far more than written); this repo relies on Neo4j's own page cache instead of a purpose-built caching layer -- see [sharding.md](sharding.md#what-this-doesnt-solve) |
| 3 | Cold feed-tier storage | Single embedded BadgerDB instance on feed-aggregation-service's local disk, no replication | Distributed LSM-tree fleet (or the doc's literal RocksDB-on-NVMe, replicated) | Losing that one disk loses cold-tier fan-out history for every dormant user it held. It's a reasonable trade *if* treated as a rebuildable cache rather than a system of record (which it should be regardless -- see [caching.md](caching.md#what-s-genuinely-different-from-real-rocksdb-on-nvme)), but a real deployment would still want it to survive a single disk failure |
| 4 | Cross-store consistency | Two named, unreconciled drift windows: the Neo4j `User` node vs. its sharded Postgres row (both written on user creation, no shared transaction), and the Redis username directory vs. source of truth | An outbox pattern (write the fact once, transactionally, to the primary store; a separate process propagates it to secondary stores/indexes with retry) | Without an outbox, "the Postgres write succeeded but the Neo4j/Redis write didn't" is a silent, permanent gap -- a user who can be found by ID but not by username, or who exists in Postgres but can't be followed. Named explicitly rather than hidden: [sharding.md](sharding.md#what-still-has-to-stay-in-sync) |
| 5 | Like/comment counter durability | Redis is the sole source of truth for `like_count`/`comment_count` -- never materialized in Postgres at all | The reference hyperscale design keeps a `like_count` column in Postgres too, kept eventually consistent via an async Kafka-batched flush from the sharded Redis counters | If Redis's AOF is lost, counts reset to zero with nothing to recover from. This repo made the more aggressive choice deliberately (see [engagement-at-scale.md](engagement-at-scale.md)) rather than half-build the flush pipeline; a real product decision here depends on whether a like count is ledger-grade data (usually: no) |
| 6 | Seen-state dedup | Redis ZSET per user, 48h TTL window | Bloom filter (or counting Bloom / Cuckoo filter) | A ZSET grows with history and needs per-user memory proportional to posts-ever-shown; a Bloom filter gives a fixed memory footprint per user regardless of how long they've been active, at the cost of a small false-positive rate (occasionally re-hiding a post that wasn't actually shown -- an acceptable trade for a feed) |
| 7 | Ranking | Rust heuristics: recency decay x normalized engagement counts | Two-pass: LightGBM/GBDT pruning (~750 -> 150 candidates) then a multi-task deep model predicting P(Like)/P(Comment)/P(Share)/P(Dwell)/P(Hide) | Heuristics can't learn interaction effects (this user tends to comment on video but not photos; this content type dwells long but converts to likes rarely) or adapt as behavior shifts -- a trained model captures patterns no hand-written formula will find, and gets better with more data instead of staying fixed |
| 8 | Out-of-network recall | Off-the-shelf sentence embeddings (MiniLM) of caption text | Trained two-tower model: one tower encodes user taste from behavior history, the other encodes item/content signals (not just text -- image embeddings, engagement velocity, author features); trained end-to-end on click/engagement labels | Caption-text similarity finds topically similar posts, not posts *this user* will engage with -- it has no signal from behavior at all. A two-tower model is trained specifically to predict engagement, and the item tower is precomputed once per item (cheap at serve time) while the user tower is refreshed as behavior changes |
| 9 | Event bus | Single Kafka broker, 3 partitions, replication factor 1 | Multi-broker cluster, replication factor 3, partition count sized to consumer parallelism needed | Replication factor 1 means the broker holding a partition is a single point of failure -- lose that box, lose unconsumed messages. Replication factor 3 + multiple brokers means the cluster survives losing any one node without data loss |
| 10 | Ranking service call | Synchronous HTTP round-trip per feed request, from feed-aggregation-service to ranking-service, for every candidate | Same shape, but usually: co-located/low-latency transport (gRPC over a service mesh, not HTTP+JSON), model inference batched and possibly cached per (user, candidate-set) for a few seconds, with a fallback path if the ranker times out | An extra network hop with JSON (de)serialization adds latency on the hot path for every single feed request; at 80k QPS that's 80k extra round-trips a second the doc's <100ms budget has to absorb. Real systems either colocate ranking with aggregation or make the RPC as cheap as physically possible, and always have a "ranker is down" fallback (e.g., recency-only ordering) rather than failing the whole request |
| 11 | Consumer error handling | Log and continue (fanout-worker, vector-pipeline); no dead-letter queue, no retry topic | Poison-message handling: retry with backoff, then route to a dead-letter topic after N failures, with alerting and a replay tool | Without this, one malformed event can either be silently dropped (data loss) or, if you naively retry forever, can wedge a consumer group's offset and stop all downstream processing behind it |
| 12 | Observability | Structured logging (`slog` in Go, standard loggers elsewhere), no metrics, no tracing | Structured logs + metrics (fan-out latency, consumer lag, cache hit rate, shard-level p50/p99, cold-tier promotion rate) + distributed tracing across the write and read paths | You cannot operate a system at 80k QPS by tailing logs. Consumer lag alone (how far behind fanout-worker is from the head of the topic) is the single most important signal for "is fan-out keeping up," and per-shard latency is what tells you *which* Postgres instance is the current bottleneck -- neither exists here |
| 13 | AuthN/AuthZ | None -- any caller can post as any `userId` | Authenticated sessions, abuse detection | Obviously required before this touches real user data; omitted here because it's orthogonal to the feed/sharding architecture itself, not because it's hard. (Engagement-specific rate limiting *is* implemented -- see [engagement-at-scale.md](engagement-at-scale.md) -- but general API-level auth is not) |
| 14 | CORS | `Access-Control-Allow-Origin: *` | Explicit allow-list of known origins | Wide-open CORS is fine for a same-machine local demo; a real deployment should never do this, since it lets any website's JS call your authenticated-looking API from a user's browser |
| 15 | Language-per-service | 6 different languages, chosen for the learning exercise | Most real orgs standardize on 1-3 languages | Polyglot has a real, non-academic cost: every language needs its own on-call runbooks, dependency-upgrade cadence, security patching process, and hiring pool -- and here specifically, the shard-routing algorithm (consistent hash + Snowflake IDs) has to be reimplemented identically in Go, Java, *and* TypeScript, which is exactly the kind of cross-language surface area that goes stale silently unless it's tested for parity on every change (`scripts/verify_shard_parity.sh` exists for exactly this reason). A team doing this in production would pay for the "best tool per job" benefit with meaningfully higher operational overhead -- worth it only when the performance/ecosystem gap between languages is large enough to justify it |
| 16 | Schema migrations | One idempotent `db/shard-schema.sql`, re-run by hand against all 4 shards | Versioned migration tool (Flyway, golang-migrate, Alembic) with up/down migrations, applied automatically and identically to every shard in CI/CD | A single idempotent script works until two people modify the schema in parallel, or a migration succeeds on 3 shards and fails on the 4th, leaving the fleet's schemas inconsistent with no record of which shards are on which version |
| 17 | Consistency model | Read-your-own-write gap: a user's own new post doesn't appear in their *own* feed read (they don't follow themselves, and it's not in a celebrity outbox unless they are one) -- only via vector recall, if at all | Real systems make an explicit product decision here (usually: show a user their own recent posts pinned/injected client-side, not through the same recall funnel) | This is a good one to notice unprompted in an interview -- it shows you're thinking about the actual user experience implications of an architecture, not just its throughput numbers |

## The five worth going deeper on

### 1. Resharding tooling (row 1)

The interview follow-up here is almost always "you have 4 shards and one
gets hot -- now what?" This repo's ID scheme is specifically designed to
make the *answer* tractable even though it doesn't implement it: shard ID
is 8 bits, independent of how many physical instances exist, so growing
from 4 to 8 shards never touches the ID format -- it only means deciding
which existing shard-ID ranges move to which new physical instances, and
actually copying that data (dual-write to old+new during migration, then
cut over, is the standard pattern). What's genuinely missing is that
migration tooling itself, and any automatic detection of "this shard is
hot" in the first place -- see [sharding.md](sharding.md#what-this-doesnt-solve).
Also worth naming unprompted: **why the social graph isn't part of this
problem at all**. A follow edge connects two users who can be on any two
of four shards -- exactly the case that makes relational resharding
brutal. Moving it to Neo4j (a real architectural change this repo made,
not a hypothetical) sidesteps that specific pain entirely, at the cost of
now also having to reshard/scale Neo4j itself someday, which is a
different, graph-specific problem.

### 2. Why Neo4j instead of sharding `follows` relationally (row 2)

The design this repo tried *first* and abandoned (visible in git history)
sharded `follows` by storing each edge twice, once per endpoint's shard,
so both "who do I follow" and "who follows me" stayed single-shard reads.
That works, but every follow/unfollow becomes a hand-rolled distributed
transaction across two independent Postgres instances -- no shared
transaction, best-effort compensation on partial failure, permanent
potential for drift between the two copies. The source doc's own "Meta
TAO" callout is itself an admission that this is the wrong tool: TAO isn't
a cleverly-sharded relational table, it's a purpose-built graph store,
because relational sharding and graph traversal want opposite things from
a key. This repo doesn't build TAO, but it makes the same underlying
choice TAO represents -- store the graph in something designed to store
graphs. Full record: [sharding.md](sharding.md#the-social-graph-lives-in-neo4j-not-sharded-postgres).

### 3. Bloom filter vs. ZSET (row 6)

If asked "why not just use the ZSET in production," the honest answer is
memory: a Bloom filter for "has user X seen post Y" needs a handful of
bits per entry regardless of how many posts have ever been shown, sized
once for a target false-positive rate. A ZSET needs an entry per
(user, post) pair, which grows without bound for an active user over
years of usage, unless you also add TTL/trimming logic -- which is
exactly what this repo does (`SEEN_STATE_TTL_SECONDS`), and is itself a
reasonable middle ground worth naming: "a bounded-TTL ZSET approximates a
Bloom filter's fixed memory footprint, trading some staleness (very old
'seen' entries silently expire and could theoretically resurface a post)
for much easier debugging."

### 4. Heuristic ranking vs. trained models (row 7)

The interview trap here is assuming "just use ML" is the whole answer.
The harder, more interesting question is *how do you get training data in
the first place* -- you need production traffic and logged
impressions/outcomes before you can train anything, which means every
real ranking system starts with a heuristic (often near-identical in
spirit to this repo's recency x engagement formula) and only replaces it
with a learned model once there's enough logged data to do so. This repo
is, in that sense, an accurate model of *stage one* of a real ranking
system's lifecycle, not a strawman.

### 5. Fan-out timing / consistency window, now across shards (row 17)

This repo's fan-out is fast enough locally that it feels synchronous, but
it's fundamentally an eventually-consistent write, and sharding adds a
second axis to it: a post exists in the author's Postgres shard before it
exists in any follower's Redis inbox, *and* fanout-worker's
active/dormant split requires a live cross-shard scatter-gather against
Postgres (grouping followers by shard, querying each concurrently) before
either write happens. A good interview answer names both gaps explicitly:
"the system trades a small, bounded staleness window for followers seeing
a new post, in exchange for not blocking post creation on fan-out work,"
and separately, "the fan-out worker's cross-shard read is itself a
partial-failure surface -- if one shard's query fails, this repo drops
just that shard's candidate followers with a logged warning rather than
failing the whole fan-out" (see [learning-notes.md](learning-notes.md),
bug #6, for the incident that motivated failing this way instead of
crashing).

## If asked "what would you build first to make this production-ready?"

Roughly in priority order, because each one either prevents data loss or
is a prerequisite for operating the system at all:

1. **Observability** (row 12) -- you can't safely change anything else,
   including resharding, without per-shard latency, consumer-lag, and
   cold-tier promotion metrics telling you if you broke it.
2. **Dead-letter handling** (row 11) -- prevents one bad message from
   silently dropping data or wedging a consumer group.
3. **Kafka replication** (row 9) -- the cheapest, highest-leverage fix for
   the single biggest data-loss risk in the current setup.
4. **Neo4j replication** (row 2) -- the social graph has zero redundancy
   today; a causal cluster is the direct analogue of Kafka replication
   for the graph store, and arguably higher priority since there's
   currently exactly one copy of "who follows whom" in the entire system.
5. **An outbox for the two named drift windows** (row 4) -- the Neo4j
   user-node sync and the username directory are the only places in this
   design where two stores can silently disagree; everywhere else either
   uses one transaction (Neo4j edges + counters) or one source of truth
   (Redis-only like counts).
6. **Resharding tooling** (row 1) -- only once one of the 4 shards is
   actually the bottleneck, since it's the most invasive operational
   change on this list.
7. **Trained ranking model** (row 7) -- requires (1) already in place to
   even know if the model is performing better than the heuristic it
   replaces.

Notice auth (row 13) isn't "last" on any real list -- it's a prerequisite
for launching to real users at all, just orthogonal to this particular
architecture discussion. Engagement-specific abuse controls (rate
limiting, moderation) are already implemented -- see
[engagement-at-scale.md](engagement-at-scale.md) -- precisely because they
aren't orthogonal to the likes/comments design the way general API auth
is orthogonal to sharding.

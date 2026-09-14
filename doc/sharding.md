# Application-Level Sharding

This document is the design record for turning the single-Postgres demo
into an actually-sharded system, per the source design doc's own call for
"horizontally sharded by `user_id`" (§2) at 500M DAU. It exists because
sharding correctly is mostly about getting the *keys* and the *cross-shard
edge cases* right, not about writing a hash function — the hash function is
the easy 5% of this problem.

## Why application-level sharding (not Vitess/Citus)

The source doc names Vitess as the production choice. This repo implements
the same idea at the application layer instead -- a `ShardRouter` and
shard-aware repositories, talking to N independent Postgres instances --
because the *point* of this exercise is to make the sharding logic visible
and readable in the service code, not hidden inside a proxy. A real team
adopting Vitess/Citus is choosing to make sharding invisible to application
code; a team choosing application-level sharding (plenty do -- it's a
legitimate, common choice, not just a demo simplification) is choosing to
own that complexity explicitly in exchange for not depending on a
specialized piece of infrastructure. Both are real, defensible choices.
This repo picks the latter because it's the one worth *showing*.

## Shard count and topology

4 physical Postgres instances (`shard-0` .. `shard-3`), identical schema on
each (see `db/shard-schema.sql`). 4 is enough to force every genuinely
cross-shard code path to actually be exercised locally (2 shards can hide
bugs that only show up with 3+), without needing 8+ containers on a laptop.

## IDs are self-routing

Every entity ID (`user_id`, `post_id`, `comment_id`) is a 64-bit Snowflake
variant that embeds its own shard number:

```
63                                          22        14         0
 | 41 bits: ms since epoch | 8 bits: shard_id | 14 bits: sequence |
```

(This is not a novel idea -- it's a close cousin of the ID scheme
Instagram's own engineering team published for exactly this reason:
https://instagram-engineering.com/sharding-ids-at-instagram-1cf5a71e5a5c.)

The consequence that matters: **routing an existing entity to its shard is
a bit-shift, never a lookup.** `shardID := (id >> 14) & 0xFF`. No directory
service, no ring lookup, no network call, and it can't go stale. 8 bits of
shard ID supports up to 256 shards without ever changing the ID format --
we run 4 today; growing to 8, 16, or 64 shards later doesn't touch this
scheme at all (it does mean *reassigning* which physical instance owns
which shard-id range, which is a separate, real migration problem --
see "What this doesn't solve" below).

## Where consistent hashing actually gets used

Given IDs are self-routing, a hash ring isn't needed to find where an
*existing* row lives. It's needed for exactly one decision: **which shard
does a brand-new entity get created on.** That's `pkg/sharding`'s job --
a CRC32-based consistent-hash ring with virtual nodes (150 per shard, so
that with only 4 real shards the load is still spread evenly rather than
landing on 4 arbitrary hash buckets), reading shard membership from
`config/shards.json`.

Consistent hashing over plain `hash(id) % N` matters here for exactly the
reason it always does: growing from 4 shards to 5 with modulo remaps
~80% of existing keys to a new home (everything's `% N` changes); a
consistent-hash ring only remaps ~1/5th of the ring's *new-entity
placement decisions* going forward -- and since existing entities never
need to be re-routed at all (their shard ID is baked into their own ID
forever), the ring only ever has to reason about where *new* signups land,
which is the one thing that's actually mutable here.

**A real measurement, not an assumed one:** `pkg/sharding/ring_test.go`
asserts both properties above rather than taking them on faith. Worth
naming a bug the test caught during development: the first vnode key
format tried was `"shard-<id>-vnode-<v>"`, which measured a 29% max
deviation across shards (CRC32 is a linear code, and hashing a constant
shard-id prefix with only the low-order vnode counter varying produced
visibly correlated hashes). Swapping the key order to
`"vnode-<v>-shard-<id>"` broke that correlation and brought max deviation
under 2% at the same 150-vnodes-per-shard setting -- same hash function,
same vnode count, only the string layout changed. Left in as a reminder
that "I used a hash function" and "I verified the output distribution"
are different claims, and only the second one is worth trusting without
checking.

Go (`pkg/sharding`) and Java (`fanout-worker`'s `ShardRing`) both
implement the identical algorithm against the identical config file --
see [`scripts/verify_shard_parity.sh`](../scripts/verify_shard_parity.sh),
which asserts both implementations agree on placement for 10,000 sample
IDs. This is load-bearing: if Go and Java ever disagreed about which
shard a new user belongs on, a signup could be created on shard 2 by one
service while another service that computes it differently later writes
related data to shard 3, silently corrupting the data model. Cross-language
parity for anything that computes routing decisions is not optional.

## Shard key per entity, and why

| Entity | Shard key | Rationale |
|---|---|---|
| `users` | own `user_id` | The root of ownership; everything else derives from this. |
| `posts` | author's shard (inherited, not re-hashed) | "Get this user's posts" and "get this post" are both then single-shard. Embedding the author's shard bits directly into the post's own ID (rather than re-running the hash) means a post's shard never needs a lookup either. |
| `comments` | **the post's shard**, not the commenter's | "List comments for post X" is the overwhelmingly dominant read pattern; co-locating with the post keeps it single-shard. The cost: "list comments I've written" (rare, not on any hot path here) becomes a scatter-gather. This is a real, deliberate trade -- optimize for the read that actually happens. |
| `follows` | **not sharded relationally at all** -- see below | The social graph lives in Neo4j instead. |
| `likes` | the liking user's shard | "Did I like these posts" is a per-viewer batch check during feed hydration -- co-locating with the *liker* makes that single-shard. "Who liked this post" (rare, used for a "liked by" list, not the hot path) becomes scatter-gather. |
| `notifications` | recipient's shard | "Get my notifications" is single-user; writing one only ever touches the recipient's shard. |

## The social graph lives in Neo4j, not sharded Postgres

A follow relationship connects two users who can be on any two of four
shards. Two read patterns need to both be cheap:
- "who does user X follow" (used to find celebrities a viewer follows)
- "who follows user Y" (used by the fan-out worker on every single post)

The first design here (kept below in git history, worth reading once)
sharded `follows` relationally and stored the edge twice -- once on each
endpoint's shard -- specifically so both directions stayed single-shard.
That works, but it's the wrong tool: it turns *every* follow/unfollow into
a hand-rolled distributed transaction (two independent Postgres instances,
no shared transaction, best-effort compensation on partial failure,
permanently-possible drift between the two copies) to simulate something
a graph database gives you for free. The source design doc's own
"Meta TAO" callout (§2) is itself already an admission of this: TAO isn't
a sharded relational table with a clever key -- it's a purpose-built
graph/association store, precisely because relational sharding and
graph traversal want opposite things from your key design.

So the social graph is **not** part of the sharded relational tier at all.
It lives in **Neo4j** (`docker-compose.yml`'s `graph-db` service):

```cypher
(:User {userId, username, followerCount, isCelebrity})
      -[:FOLLOWS {createdAt}]->
(:User)
```

- "Who does X follow": `MATCH (:User {userId:$x})-[:FOLLOWS]->(f) RETURN f`
- "Who follows Y" (fan-out's hot path): `MATCH (follower)-[:FOLLOWS]->(:User {userId:$y}) RETURN follower`

Both directions are native O(degree) traversals -- Neo4j stores each
relationship once, indexed from both ends, so there is no dual-write to
get wrong and no drift to reconcile. `followerCount` and `isCelebrity` are
properties on the `User` node, updated in the *same* Cypher transaction
that creates or deletes a `FOLLOWS` edge -- one graph database, one
transaction, no cross-store consistency problem, unlike the counters
discussed below (which genuinely do straddle two different stores).

This is a straightforwardly better fit, not a trade-off with a downside
to hide: the only real cost is operating a second stateful system
(Neo4j) alongside sharded Postgres and Redis, which is exactly the cost
TAO itself pays in the original architecture, and exactly why the doc
calls it out as its own row in the data-store table rather than folding
it into the primary database.

### What still has to stay in sync

`User` nodes in Neo4j need to exist before a `FOLLOWS` edge can attach to
them, so `post-ingestion-service` `MERGE`s a lightweight `User` node
(`userId`, `username`) into Neo4j in the same request that creates the
user in sharded Postgres. `MERGE` (Neo4j's upsert) makes this safe to
retry, but it is still two independent writes to two independent
systems with no shared transaction -- the same class of gap as the
username directory below, and named the same way: if the Neo4j write
fails after the Postgres write succeeds, that user can post and be
followed-by-ID but won't appear as followable-by-username until
reconciled. Worth fixing with an outbox before this is real; not
fixed here, and not hidden either.

## Counters live in Redis, not the sharded database

`like_count` and `comment_count` have the same cross-shard problem as
`follows`, for a different reason: a like is *written* on the liker's
shard (see table above) but the *counter* needs to live wherever reads of
the post happen -- which is the post's shard. Liker and author are
frequently on different shards, so incrementing `posts.like_count`
transactionally alongside the `likes` row insert is, again, a cross-shard
write.

Rather than apply the same dual-write-with-compensation pattern to a
value that changes far more often than a follow edge does (posts get
liked repeatedly; follow edges are created once), counters are moved out
of Postgres entirely: `INCR likes:count:<post_id>` /
`INCR comments:count:<post_id>` in Redis. This sidesteps the cross-shard
write completely -- Redis is not sharded by user in this design, so there
is exactly one place a counter lives, reachable in one round trip
regardless of which shard the liker or the post's author is on. It also
matches how large-scale systems actually handle hot counters (approximate,
fast, separate from the system of record), and it's *consistent* with
this repo's own pre-existing design: the feed read path was already
Redis-heavy (inboxes, outboxes, seen-state) specifically to keep the hot
path off Postgres.

The `likes` and `comments` **rows** (who liked what, actual comment text)
remain in sharded Postgres as the system of record -- only the fast-moving
aggregate counts moved to Redis. `ranking-service` and the feed hydration
step read counts from Redis, not from a Postgres column.

## Cross-shard fan-out queries (the ones that remain)

Not every cross-shard query is solvable by a key redesign -- some are
inherently global. These are implemented as explicit scatter-gather
(group affected IDs by shard using the self-routing bit-shift, fire
requests to each shard's connection pool concurrently, merge), not
hidden behind an ORM that makes it look like a single query:

- **"Recent posts by this viewer or the people they follow"** (taste-
  vector seeding for vector recall): the followee list itself is one
  Neo4j query (see above); their *posts* are still in sharded Postgres,
  co-located by author, so this groups the returned followee IDs by
  shard (bit-shift, no lookup), fans out in parallel, and merges by
  `created_at`. This is the one cross-shard scatter-gather that a graph
  database doesn't remove, because it's fundamentally about post
  storage, not about the graph.
- **"List all users"** (the web UI's People panel): inherently global,
  no key redesign fixes it. Fans out to all 4 shards and merges. This is
  explicitly a demo/admin-scale query -- at real scale you'd never list
  "all users" unbounded like this at all; you'd search/paginate through a
  purpose-built index (Elasticsearch, or a dedicated directory service),
  not the sharded primary store.

## The username problem (a global secondary index)

`@mention` resolution and the People panel both need "look up user_id by
username" -- but username isn't the shard key, so there's no bit-shift
shortcut and no reasonable way to guess which shard a username lives on.
This is the classic global-secondary-index problem sharding always
creates for any non-shard-key lookup.

Solution: a Redis-backed directory, `username:<name> -> user_id`, written
once at signup alongside the sharded `INSERT`. It's an extra place that
can theoretically drift from the source of truth (same category of gap as
the Neo4j `User` node sync above), but it turns the single hottest
non-shard-key lookup in the system into one Redis `GET` instead of a
4-way fan-out on every single post containing a mention.

## What this doesn't solve

For the record, since a principal-engineer-level review should name the
gaps as clearly as the solutions:
- **No resharding tooling.** Growing from 4 to N shards means deciding
  which existing shard-id ranges move to new physical instances and
  actually moving that data -- this repo has the ID scheme that makes
  that *tractable* (shard id is 8 bits, independent of instance count),
  not the migration tooling that would *execute* it.
- **No cross-shard transactions** for the relational tier, full stop --
  Redis counters and Neo4j's own transactions cover this system's actual
  hot cross-shard-shaped writes, but that's a consequence of picking the
  right store per access pattern, not a general solution to distributed
  transactions.
- **No automatic reconciliation** for the two named drift windows: the
  Neo4j `User` node vs. its sharded Postgres row, and the username
  directory vs. source of truth. Named as explicit follow-up work
  (an outbox pattern is the standard fix for both), not silently ignored.
- **Neo4j itself isn't sharded or replicated here** -- one instance, one
  point of failure for the entire social graph. A real deployment would
  run it in a causal cluster (Neo4j's own replication story) for the same
  reason Kafka needs replication factor > 1 elsewhere in this system: a
  single-instance stateful service is a single point of failure regardless
  of which product it is.
